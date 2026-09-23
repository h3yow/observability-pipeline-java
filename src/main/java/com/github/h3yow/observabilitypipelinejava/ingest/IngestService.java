package com.github.h3yow.observabilitypipelinejava.ingest;

import com.github.h3yow.observabilitypipelinejava.aggregate.WindowAggregator;
import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.guard.CardinalityGuard;
import com.github.h3yow.observabilitypipelinejava.guard.RateLimiter;
import com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity;
import com.github.h3yow.observabilitypipelinejava.model.DegradeReason;
import com.github.h3yow.observabilitypipelinejava.model.IngestResult;
import com.github.h3yow.observabilitypipelinejava.model.IngestStatus;
import com.github.h3yow.observabilitypipelinejava.model.MetricSample;
import com.github.h3yow.observabilitypipelinejava.model.PressureSnapshot;
import com.github.h3yow.observabilitypipelinejava.storage.FileSampleStore;
import com.github.h3yow.observabilitypipelinejava.storage.StoredSample;
import com.github.h3yow.observabilitypipelinejava.storage.SampleStore;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
import com.github.h3yow.observabilitypipelinejava.time.WindowTiming;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 采集入口：校验 -&gt; 限流 -&gt; 时间分类 -&gt; 基数 -&gt; 去重 -&gt; 落盘 -&gt; 聚合。
 * 任何输入都返回可区分的受理/拒绝结果，绝不静默丢弃。
 */
public class IngestService {

    private final ObservabilityProperties properties;
    private final WindowTiming timing;
    private final Clock clock;
    private final SampleStore store;
    private final WindowAggregator aggregator;
    private final CardinalityGuard cardinalityGuard;
    private final RateLimiter rateLimiter;

    private final AtomicLong acceptedTotal = new AtomicLong();
    private final AtomicLong acceptedLateTotal = new AtomicLong();
    private final AtomicLong duplicateTotal = new AtomicLong();
    private final AtomicLong rejectedTotal = new AtomicLong();
    private final Map<IngestStatus, AtomicLong> byStatus = new EnumMap<>(IngestStatus.class);

    public IngestService(ObservabilityProperties properties,
                         WindowTiming timing,
                         Clock clock,
                         SampleStore store,
                         WindowAggregator aggregator,
                         CardinalityGuard cardinalityGuard,
                         RateLimiter rateLimiter) {
        this.properties = properties;
        this.timing = timing;
        this.clock = clock;
        this.store = store;
        this.aggregator = aggregator;
        this.cardinalityGuard = cardinalityGuard;
        this.rateLimiter = rateLimiter;
        for (IngestStatus s : IngestStatus.values()) {
            byStatus.put(s, new AtomicLong());
        }
    }

    /** 启动重放：恢复历史窗口聚合与基数注册（不落盘、不计数）。 */
    public synchronized void prime() {
        if (store instanceof FileSampleStore fs) {
            List<StoredSample> history = fs.prime();
            for (StoredSample s : history) {
                cardinalityGuard.admitAndRegister(s.metric(), s.fingerprint());
                aggregator.apply(s);
            }
        }
    }

    /** 上报单条样本。 */
    public IngestResult ingest(MetricSample sample) {
        long now = clock.nowMillis();

        // 1) 输入校验（指标名/数值/标签结构）
        if (sample == null || sample.metric() == null || sample.metric().isBlank()) {
            return reject(IngestStatus.INVALID_METRIC, null, null,
                    "metric name is required", now, sample);
        }
        String metric = sample.metric().trim();
        if (!isValidMetricName(metric)) {
            return reject(IngestStatus.INVALID_METRIC, null, null,
                    "illegal metric name: " + metric, now, sample);
        }
        if (Double.isNaN(sample.value()) || Double.isInfinite(sample.value())) {
            return reject(IngestStatus.INVALID_VALUE, null, null,
                    "value must be finite, got " + sample.value(), now, sample);
        }
        if (sample.labels() != null) {
            String labelProblem = validateLabels(sample.labels());
            if (labelProblem != null) {
                return reject(IngestStatus.INVALID_LABELS, null, null,
                        labelProblem, now, sample);
            }
        }

        // 2) 时间戳：null 由服务端补齐；异常时间戳拒绝
        long ts;
        if (sample.timestampMs() == null) {
            ts = now;
        } else {
            ts = sample.timestampMs();
            if (ts <= 0) {
                return reject(IngestStatus.INVALID_TIMESTAMP, null, null,
                        "timestamp must be positive epoch millis, got " + ts, now, sample);
            }
        }
        WindowTiming.TsClass tsClass = timing.classify(ts, now);
        if (tsClass == WindowTiming.TsClass.TOO_FAR_FUTURE) {
            return reject(IngestStatus.TOO_FAR_FUTURE, null, null,
                    "timestamp " + ts + " exceeds now+" + timing.futureSkewMs()
                            + " (now=" + now + ")", now, sample);
        }
        if (tsClass == WindowTiming.TsClass.TOO_OLD) {
            return reject(IngestStatus.TOO_OLD, null, null,
                    "timestamp " + ts + " is older than retention/lateness bound (now="
                            + now + ")", now, sample);
        }

        // 3) 速率配额
        if (!rateLimiter.tryAcquire()) {
            return reject(IngestStatus.RATE_LIMITED, null, null,
                    "ingest rate exceeds " + properties.getRateLimitPerSec() + "/s", now, sample);
        }

        // 4) 序列身份与基数配额（已注册序列永远放行）
        Map<String, String> normalized = SeriesIdentity.normalize(sample.labels());
        String fingerprint = SeriesIdentity.fingerprint(normalized);
        String seriesKey = SeriesIdentity.seriesKey(metric, fingerprint);
        CardinalityGuard.Decision admit = cardinalityGuard.admitAndRegister(metric, fingerprint);
        if (!admit.allowed()) {
            IngestStatus status = admit.reason() == DegradeReason.METRIC_CARDINALITY_EXCEEDED
                    ? IngestStatus.CARDINALITY_LIMIT : IngestStatus.GLOBAL_CARDINALITY_LIMIT;
            return reject(status, seriesKey, timing.windowStartFor(ts),
                    admit.detail(), now, sample);
        }

        String sourceId = sample.sourceId() == null || sample.sourceId().isBlank()
                ? "anonymous" : sample.sourceId().trim();
        StoredSample candidate = new StoredSample(-1L, metric, normalized, fingerprint,
                ts, sample.value(), sourceId, now, tsClass == WindowTiming.TsClass.LATE);

        // 5) 窗口内去重（落盘前判定，重复样本不消耗接收序号）
        if (aggregator.isDuplicate(candidate)) {
            return reject(IngestStatus.DUPLICATE, seriesKey,
                    timing.windowStartFor(ts),
                    "duplicate sample (metric+labels+ts+value+source) in window",
                    now, sample);
        }

        // 6) 落盘分配接收序号 -> 7) 聚合
        StoredSample stored = store.append(candidate);
        WindowAggregator.ApplyResult applied = aggregator.apply(stored);
        if (applied == WindowAggregator.ApplyResult.DUPLICATE) {
            // 极端竞争：另一线程在去重探测后先写入相同样本。
            return reject(IngestStatus.DUPLICATE, seriesKey,
                    timing.windowStartFor(ts),
                    "duplicate detected concurrently at aggregate step", now, sample);
        }
        if (applied == WindowAggregator.ApplyResult.REJECTED_SEALED) {
            // 极端竞争：探测窗口刚好被封存。样本已落盘可追溯，但不参与聚合。
            return reject(IngestStatus.TOO_OLD, seriesKey, timing.windowStartFor(ts),
                    "window sealed concurrently; sample persisted but not aggregated", now, sample);
        }

        long windowStart = timing.windowStartFor(ts);
        boolean late = tsClass == WindowTiming.TsClass.LATE;
        acceptedTotal.incrementAndGet();
        byStatus.get(late ? IngestStatus.ACCEPTED_LATE : IngestStatus.ACCEPTED).incrementAndGet();
        if (late) {
            acceptedLateTotal.incrementAndGet();
        }
        String detail = (late ? "accepted as late into closed window "
                : "accepted into open window ") + windowStart;
        return new IngestResult(late ? IngestStatus.ACCEPTED_LATE : IngestStatus.ACCEPTED,
                true, seriesKey, windowStart, detail, stored.receiveSeq());
    }

    /** 批量上报（逐条独立判定，单条异常不影响其他样本）。 */
    public List<IngestResult> ingestBatch(List<MetricSample> samples) {
        List<IngestResult> results = new ArrayList<>();
        if (samples != null) {
            for (MetricSample s : samples) {
                results.add(ingest(s));
            }
        }
        return results;
    }

    /** 自观测压力快照。 */
    public PressureSnapshot pressure() {
        Map<IngestStatus, Long> rejections = new EnumMap<>(IngestStatus.class);
        long rejected = 0L;
        for (Map.Entry<IngestStatus, AtomicLong> e : byStatus.entrySet()) {
            long v = e.getValue().get();
            rejections.put(e.getKey(), v);
            if (e.getKey() != IngestStatus.ACCEPTED
                    && e.getKey() != IngestStatus.ACCEPTED_LATE) {
                rejected += v;
            }
        }
        boolean metricLimited = cardinalityGuard.currentPerMetric().entrySet().stream()
                .anyMatch(en -> en.getValue() >= properties.getMaxSeriesPerMetric());
        boolean globalLimited = cardinalityGuard.currentGlobalCount()
                >= properties.getMaxGlobalSeries();
        boolean rateLimitedNow = rateLimiter.isLimitedNow();
        DegradeReason primary;
        if (globalLimited) {
            primary = DegradeReason.GLOBAL_CARDINALITY_EXCEEDED;
        } else if (metricLimited) {
            primary = DegradeReason.METRIC_CARDINALITY_EXCEEDED;
        } else if (rateLimitedNow) {
            primary = DegradeReason.RATE_EXCEEDED;
        } else {
            primary = DegradeReason.NONE;
        }
        return new PressureSnapshot(clock.nowMillis(),
                acceptedTotal.get(), acceptedLateTotal.get(), duplicateTotal.get(),
                rejected, rejections,
                cardinalityGuard.currentGlobalCount(),
                cardinalityGuard.currentPerMetric(),
                properties.getMaxSeriesPerMetric(),
                properties.getMaxGlobalSeries(),
                rateLimiter.currentRatePerSec(),
                properties.getRateLimitPerSec(),
                metricLimited, globalLimited, rateLimitedNow, primary);
    }

    private IngestResult reject(IngestStatus status, String seriesKey, Long windowStart,
                                String detail, long now, MetricSample sample) {
        rejectedTotal.incrementAndGet();
        byStatus.get(status).incrementAndGet();
        if (status == IngestStatus.DUPLICATE) {
            duplicateTotal.incrementAndGet();
        }
        return new IngestResult(status, false, seriesKey, windowStart, detail, -1L);
    }

    private boolean isValidMetricName(String name) {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-';
            if (!ok) {
                return false;
            }
        }
        return !name.isEmpty() && name.length() <= 256;
    }

    private String validateLabels(Map<String, String> labels) {
        if (labels.size() > properties.getMaxLabelCount()) {
            return "label count " + labels.size() + " > max " + properties.getMaxLabelCount();
        }
        for (Map.Entry<String, String> e : labels.entrySet()) {
            if (e.getKey() == null || e.getKey().isBlank()) {
                return "label key must not be blank";
            }
            if (e.getKey().length() > properties.getMaxLabelKeyLength()) {
                return "label key too long: " + e.getKey();
            }
            String v = e.getValue() == null ? "" : e.getValue();
            if (v.length() > properties.getMaxLabelValueLength()) {
                return "label value too long for key " + e.getKey().trim();
            }
        }
        return null;
    }
}
