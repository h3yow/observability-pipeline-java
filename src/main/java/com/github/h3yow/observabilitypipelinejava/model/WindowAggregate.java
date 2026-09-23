package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 单个序列单个窗口的聚合累积器（线程安全，随窗口封存而冻结）。
 *
 * <p>可复现性保证：
 * <ul>
 *   <li>COUNT/SUM/MIN/MAX 天然与写入顺序无关；</li>
 *   <li>LAST 按（样本时间戳, 接收序号）决胜，乱序写入结果相同；</li>
 *   <li>重复样本（同序列、同时间戳、同数值、同来源）写入前在窗口内去重；</li>
 *   <li>封存（{@link #seal}）后任何写入被拒绝，聚合值冻结。</li>
 * </ul>
 */
public final class WindowAggregate {

    private final String metric;
    private final Map<String, String> labels;
    private final String fingerprint;
    private final String seriesKey;
    private final long windowStartMs;
    private final long windowSizeMs;

    private WindowState state = WindowState.OPEN;
    private long sealedAtMs = -1L;

    private long count = 0L;
    private double sum = 0d;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double lastValue = Double.NaN;
    private long lastTs = Long.MIN_VALUE;
    private long lastSeq = Long.MIN_VALUE;

    /** 窗口内重复样本集合：key = (时间戳, 数值位, 来源)。 */
    private final Set<DedupeKey> seen = new HashSet<>();

    public WindowAggregate(String metric, Map<String, String> labels, String fingerprint,
                           String seriesKey, long windowStartMs, long windowSizeMs) {
        this.metric = Objects.requireNonNull(metric, "metric");
        this.labels = Collections.unmodifiableMap(new java.util.TreeMap<>(
                labels == null ? Map.of() : labels));
        this.fingerprint = fingerprint;
        this.seriesKey = seriesKey;
        this.windowStartMs = windowStartMs;
        this.windowSizeMs = windowSizeMs;
    }

    /** 尝试写入样本；false 表示窗口已封存或窗口内重复。 */
    public synchronized boolean apply(StoredSampleView sample) {
        if (state == WindowState.SEALED) {
            return false;
        }
        DedupeKey key = new DedupeKey(sample.timestampMs(),
                Double.doubleToRawLongBits(sample.value()),
                sample.sourceId() == null ? "" : sample.sourceId());
        if (!seen.add(key)) {
            return false;
        }
        count++;
        sum += sample.value();
        min = Math.min(min, sample.value());
        max = Math.max(max, sample.value());
        // LAST：时间戳优先，同时间戳取接收序号更大者，与到达顺序无关。
        if (sample.timestampMs() > lastTs
                || (sample.timestampMs() == lastTs && sample.receiveSeq() > lastSeq)) {
            lastTs = sample.timestampMs();
            lastSeq = sample.receiveSeq();
            lastValue = sample.value();
        }
        return true;
    }

    /** 冻结窗口，聚合值不再变化。 */
    public synchronized void seal(long sealedAtMs) {
        if (state == WindowState.OPEN) {
            state = WindowState.SEALED;
            this.sealedAtMs = sealedAtMs;
            seen.clear(); // 封存后不可能再写入，释放去重集合占用的内存。
        }
    }

    public synchronized boolean isSealed() {
        return state == WindowState.SEALED;
    }

    public synchronized double valueOf(AggregateFunction function) {
        return switch (function) {
            case COUNT -> count;
            case SUM -> sum;
            case AVG -> count == 0 ? Double.NaN : sum / count;
            case MIN -> count == 0 ? Double.NaN : min;
            case MAX -> count == 0 ? Double.NaN : max;
            case LAST -> lastValue;
        };
    }

    public synchronized long count() {
        return count;
    }

    public String getSeriesKey() {
        return seriesKey;
    }

    public long getWindowStartMs() {
        return windowStartMs;
    }

    public long getWindowSizeMs() {
        return windowSizeMs;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public synchronized WindowState getState() {
        return state;
    }

    public synchronized long getSealedAtMs() {
        return sealedAtMs;
    }

    private record DedupeKey(long timestampMs, long valueBits, String sourceId) {
    }

    /** 样本是否为窗口内重复（不写入）。 */
    public synchronized boolean isDuplicate(StoredSampleView sample) {
        if (state == WindowState.SEALED) {
            return false;
        }
        return seen.contains(new DedupeKey(sample.timestampMs(),
                Double.doubleToRawLongBits(sample.value()),
                sample.sourceId() == null ? "" : sample.sourceId()));
    }

    /** 只暴露判定所需字段，避免聚合器直接依赖存储类型。 */
    public interface StoredSampleView {
        long timestampMs();
        double value();
        String sourceId();
        long receiveSeq();
    }
}
