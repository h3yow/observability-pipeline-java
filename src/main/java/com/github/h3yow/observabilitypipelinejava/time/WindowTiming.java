package com.github.h3yow.observabilitypipelinejava.time;

/**
 * 时间窗口与迟到/异常边界计算。
 *
 * <p>固定窗口按 epoch 对齐：{@code start = floor(ts / size) * size}，
 * 同一样本无论何时到达都落入同一窗口，这是聚合可复现的基础。
 *
 * <p>以 now 为基准，样本时间戳分为四类（边界均可解释）：
 * <ul>
 *   <li>{@link TsClass#NORMAL}：时间戳落在当前打开窗口或未来允许偏差内；</li>
 *   <li>{@link TsClass#LATE}：窗口已关闭但未超过迟到宽限期 latenessMs，可补写；</li>
 *   <li>{@link TsClass#TOO_OLD}：早于 {@code now - retentionMs}，窗口不可再补写；</li>
 *   <li>{@link TsClass#TOO_FAR_FUTURE}：晚于 {@code now + futureSkewMs}，判定为异常时间戳。</li>
 * </ul>
 * 注意：latenessMs &lt;= retentionMs，因此 LATE 窗口必然仍在保留期内。
 */
public final class WindowTiming {

    private final long windowSizeMs;
    private final long latenessMs;
    private final long futureSkewMs;
    private final long retentionMs;

    public WindowTiming(long windowSizeMs, long latenessMs, long futureSkewMs, long retentionMs) {
        if (windowSizeMs <= 0) {
            throw new IllegalArgumentException("windowSizeMs must be positive: " + windowSizeMs);
        }
        if (latenessMs < 0) {
            throw new IllegalArgumentException("latenessMs must be >= 0: " + latenessMs);
        }
        if (futureSkewMs < 0) {
            throw new IllegalArgumentException("futureSkewMs must be >= 0: " + futureSkewMs);
        }
        if (retentionMs < latenessMs) {
            throw new IllegalArgumentException(
                    "retentionMs must be >= latenessMs: " + retentionMs + " < " + latenessMs);
        }
        this.windowSizeMs = windowSizeMs;
        this.latenessMs = latenessMs;
        this.futureSkewMs = futureSkewMs;
        this.retentionMs = retentionMs;
    }

    /** epoch 对齐的窗口起点。 */
    public long windowStartFor(long timestampMs) {
        return Math.floorDiv(timestampMs, windowSizeMs) * windowSizeMs;
    }

    public long windowEndMs(long windowStartMs) {
        return windowStartMs + windowSizeMs;
    }

    /** 时间戳合法性分类。 */
    public TsClass classify(long timestampMs, long nowMs) {
        long start = windowStartFor(timestampMs);
        long currentStart = windowStartFor(nowMs);
        if (timestampMs > nowMs + futureSkewMs) {
            return TsClass.TOO_FAR_FUTURE;
        }
        if (timestampMs < nowMs - retentionMs) {
            return TsClass.TOO_OLD;
        }
        if (start < currentStart) {
            // 已过保留期的情况已在上面排除；这里只需区分是否仍在迟到宽限期内。
            if (windowEndMs(start) + latenessMs <= nowMs) {
                // 超出迟到宽限期但仍在保留期内：按 TOO_OLD 拒绝（不再可补写）。
                return TsClass.TOO_OLD;
            }
            return TsClass.LATE;
        }
        return TsClass.NORMAL;
    }

    /**
     * 可封存窗口的最大窗口起点：
     * {@code end + lateness <= now}，即 {@code start <= now - lateness - size}。
     * 严格小于当前窗口。
     */
    public long sealableHorizonStart(long nowMs) {
        long lastStart = windowStartFor(nowMs - latenessMs - windowSizeMs);
        return Math.min(lastStart, windowStartFor(nowMs) - windowSizeMs);
    }

    /** 可驱逐窗口的最大窗口起点：{@code end + retention <= now}。 */
    public long evictHorizonStart(long nowMs) {
        return windowStartFor(nowMs - retentionMs - windowSizeMs);
    }

    public long windowSizeMs() {
        return windowSizeMs;
    }

    public long latenessMs() {
        return latenessMs;
    }

    public long futureSkewMs() {
        return futureSkewMs;
    }

    public long retentionMs() {
        return retentionMs;
    }

    /** 时间戳合法性分类结果。 */
    public enum TsClass { NORMAL, LATE, TOO_OLD, TOO_FAR_FUTURE }
}
