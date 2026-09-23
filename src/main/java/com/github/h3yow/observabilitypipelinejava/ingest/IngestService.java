package com.github.h3yow.observabilitypipelinejava.ingest;

import com.github.h3yow.observabilitypipelinejava.config.PipelineProperties;
import com.github.h3yow.observabilitypipelinejava.domain.IngestResult;
import com.github.h3yow.observabilitypipelinejava.domain.IngestStatus;
import com.github.h3yow.observabilitypipelinejava.domain.MetricSample;
import com.github.h3yow.observabilitypipelinejava.domain.RejectReason;
import com.github.h3yow.observabilitypipelinejava.service.PipelineStats;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
import com.github.h3yow.observabilitypipelinejava.window.AddOutcome;
import com.github.h3yow.observabilitypipelinejava.window.SeriesKey;
import com.github.h3yow.observabilitypipelinejava.window.WindowMath;
import com.github.h3yow.observabilitypipelinejava.window.WindowStore;

/**
 * 指标受理入口：校验异常输入、限流、窗口归属与迟到/乱序/重复判定，
 * 写入 {@link WindowStore}。每一种拒绝都有可区分原因并计入 {@link PipelineStats}。
 *
 * <p>校验顺序（任何一步失败立即返回，原因互斥可解释）：
 * <ol>
 *   <li>样本/指标名/标签键/数值/时间戳合法性</li>
 *   <li>时间戳未来钟偏</li>
 *   <li>时间戳早于历史下界（保留期之前）</li>
 *   <li>速率配额</li>
 *   <li>窗口是否已封板且超宽限期</li>
 *   <li>序列基数（新序列才受限；老序列继续可写，保证存活流量不受高基数影响）</li>
 *   <li>窗口内重复样本</li>
 * </ol>
 */
public class IngestService {

    private final WindowStore store;
    private final RateLimiter rateLimiter;
    private final PipelineProperties props;
    private final Clock clock;
    private final PipelineStats stats;

    public IngestService(WindowStore store, RateLimiter rateLimiter, PipelineProperties props,
                         Clock clock, PipelineStats stats) {
        this.store = store;
        this.rateLimiter = rateLimiter;
        this.props = props;
        this.clock = clock;
        this.stats = stats;
    }

    /** 受理单条样本，返回确定且可解释的结论。 */
    public IngestResult ingest(MetricSample sample) {
        if (sample == null) {
            return reject(RejectReason.EMPTY_METRIC_NAME, -1L, "sample is null");
        }
        String metric = sample.metric();
        if (metric == null || metric.isBlank()) {
            return reject(RejectReason.EMPTY_METRIC_NAME, -1L, "metric name is empty");
        }
        if (sample.labels() != null) {
            for (String key : sample.labels().keySet()) {
                if (key == null || key.isEmpty()) {
                    return reject(RejectReason.EMPTY_LABEL_KEY, -1L,
                            "label key must not be empty, metric=" + metric);
                }
            }
        }
        if (!Double.isFinite(sample.value())) {
            return reject(RejectReason.NON_FINITE_VALUE, -1L,
                    "value must be finite, metric=" + metric + ", value=" + sample.value());
        }
        long ts = sample.timestamp();
        long now = clock.nowMillis();
        if (ts > now + props.getMaxFutureSkewMillis()) {
            return reject(RejectReason.TIMESTAMP_IN_FUTURE, -1L,
                    "timestamp " + ts + " exceeds allowed future skew " + props.getMaxFutureSkewMillis());
        }
        long size = props.getWindowSizeMillis();
        long grace = props.getGracePeriodMillis();
        long retention = props.getRetentionMillis();
        long oldestAccepted = now - retention;
        if (ts < oldestAccepted) {
            return reject(RejectReason.TIMESTAMP_TOO_OLD, WindowMath.windowStart(ts, size),
                    "timestamp " + ts + " older than retention horizon " + oldestAccepted);
        }

        if (!rateLimiter.tryAcquire()) {
            stats.markRateDegraded();
            return reject(RejectReason.RATE_LIMITED, WindowMath.windowStart(ts, size),
                    "rate limit " + rateLimiter.limit() + "/s exceeded");
        }

        long ws = WindowMath.windowStart(ts, size);
        long sealedAt = ws + size + grace;
        boolean late = now >= ws + size;
        if (now >= sealedAt) {
            return reject(RejectReason.WINDOW_CLOSED, ws,
                    "window " + ws + " sealed at " + sealedAt + ", now=" + now);
        }

        SeriesKey key = SeriesKey.of(metric, sample.labels());
        // 乱序判定必须基于"写入前"该序列已有的最大时间戳
        long prevMax = store.maxTimestampSeen(key);
        AddOutcome outcome = store.addSample(key, ws, ts, sample.value());
        if (outcome == AddOutcome.GLOBAL_LIMIT) {
            stats.markCardinalityDegraded(PipelineStats.DEGRADE_CARDINALITY_GLOBAL);
            return reject(RejectReason.GLOBAL_CARDINALITY_LIMIT, ws,
                    "global series limit " + props.getMaxSeriesGlobal() + " reached");
        }
        if (outcome == AddOutcome.SERIES_LIMIT) {
            stats.markCardinalityDegraded(PipelineStats.DEGRADE_CARDINALITY_PER_METRIC);
            return reject(RejectReason.CARDINALITY_LIMIT, ws,
                    "per-metric series limit " + props.getMaxSeriesPerMetric()
                            + " reached for metric " + metric);
        }
        if (outcome == AddOutcome.DUPLICATE) {
            stats.recordDuplicate();
            return new IngestResult(IngestStatus.REJECTED, RejectReason.DUPLICATE, ws,
                    false, false,
                    "duplicate sample in window " + ws + ": metric=" + metric + " ts=" + ts
                            + " value=" + sample.value());
        }

        // ADDED：迟到与乱序是两个可同时成立的受理标记
        boolean outOfOrder = prevMax != Long.MIN_VALUE && ts < prevMax;
        if (outOfOrder) {
            stats.recordOutOfOrder();
        }
        if (late) {
            stats.recordAcceptedLate();
            return new IngestResult(IngestStatus.ACCEPTED_LATE, RejectReason.NONE, ws, true,
                    outOfOrder,
                    "accepted late into sealed-but-grace window " + ws
                            + (outOfOrder ? " (also out-of-order vs previous max " + prevMax + ")" : ""));
        }
        stats.recordAccepted();
        return new IngestResult(IngestStatus.ACCEPTED, RejectReason.NONE, ws, false, outOfOrder,
                "accepted into open window " + ws
                        + (outOfOrder ? " (out-of-order vs previous max " + prevMax + ")" : ""));
    }

    private IngestResult reject(RejectReason reason, long windowStart, String detail) {
        stats.recordRejected(reason);
        return new IngestResult(IngestStatus.REJECTED, reason, windowStart, false, false, detail);
    }
}
