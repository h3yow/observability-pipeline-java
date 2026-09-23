package com.github.h3yow.observabilitypipelinejava.window;

/**
 * 固定对齐、固定长度的滚动窗口计算。窗口按 epoch 整除对齐，
 * 因此同一时间戳在任何时刻、任何节点上都归属同一个窗口——聚合可重复复现。
 */
public final class WindowMath {

    private WindowMath() {
    }

    /**
     * 时间戳所属窗口起点（floor 对齐）。
     * 对负时间戳也使用 floor 语义（Math.floorDiv），保证窗口连续无重叠。
     */
    public static long windowStart(long timestampMillis, long windowSizeMillis) {
        if (windowSizeMillis <= 0L) {
            throw new IllegalArgumentException("windowSizeMillis must be positive: " + windowSizeMillis);
        }
        return Math.floorDiv(timestampMillis, windowSizeMillis) * windowSizeMillis;
    }

    /** 窗口结束（不含）。 */
    public static long windowEnd(long windowStartMillis, long windowSizeMillis) {
        return windowStartMillis + windowSizeMillis;
    }
}
