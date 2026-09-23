package com.github.h3yow.observabilitypipelinejava.window;

import com.github.h3yow.observabilitypipelinejava.domain.WindowAggregate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 单个（序列, 窗口）的样本容器与确定性聚合器。
 * 聚合值仅由窗口内样本集合决定：内部按 (时间戳, 值, 到达序号) 排序后计算，
 * 迟到/乱序写入后重算结果与按序到达完全一致。
 */
public class WindowBucket {

    /** 单条样本：时间戳 + 值 + 到达序号（同时间戳同值时用于稳定排序）。 */
    private record Point(long timestamp, double value, long arrivalSeq) {
    }

    private static final Comparator<Point> POINT_ORDER =
            Comparator.comparingLong(Point::timestamp)
                    .thenComparingDouble(Point::value)
                    .thenComparingLong(Point::arrivalSeq);

    private final SeriesKey key;
    private final long windowStart;
    private final List<Point> points = new ArrayList<>();
    private long arrivalCounter = 0L;

    public WindowBucket(SeriesKey key, long windowStart) {
        this.key = key;
        this.windowStart = windowStart;
    }

    public SeriesKey key() {
        return key;
    }

    public long windowStart() {
        return windowStart;
    }

    public synchronized long lastTimestamp() {
        long max = Long.MIN_VALUE;
        for (Point p : points) {
            if (p.timestamp() > max) {
                max = p.timestamp();
            }
        }
        return max;
    }

    public synchronized int sampleCount() {
        return points.size();
    }

    /** 是否已经包含 (timestamp,value) 完全相同的样本（按需求做重复判定）。 */
    public synchronized boolean contains(long timestampMillis, double value) {
        for (Point p : points) {
            if (p.timestamp() == timestampMillis && Double.compare(p.value(), value) == 0) {
                return true;
            }
        }
        return false;
    }

    /** 加入样本；完全重复返回 false。 */
    public synchronized boolean add(double value, long timestampMillis) {
        if (contains(timestampMillis, value)) {
            return false;
        }
        points.add(new Point(timestampMillis, value, arrivalCounter++));
        return true;
    }

    /**
     * 计算聚合；结果确定可复现：先对样本做全排序，再顺序累积，
     * 浮点求和采用升序求和（输入顺序确定 => 舍入确定）。
     */
    public synchronized WindowAggregate aggregate(long windowSizeMillis) {
        List<Point> sorted = new ArrayList<>(points);
        sorted.sort(POINT_ORDER);

        long count = sorted.size();
        if (count == 0L) {
            return new WindowAggregate(key.metric(), key.labels(), windowStart,
                    windowStart + windowSizeMillis, 0L, 0d, 0d, 0d, 0d, Long.MIN_VALUE, false);
        }

        double sum = 0d;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long lastTs = Long.MIN_VALUE;
        for (Point p : sorted) {
            sum += p.value();
            min = Math.min(min, p.value());
            max = Math.max(max, p.value());
            lastTs = Math.max(lastTs, p.timestamp());
        }
        double avg = sum / count;
        return new WindowAggregate(key.metric(), key.labels(), windowStart,
                windowStart + windowSizeMillis, count, sum, min, max, avg, lastTs, false);
    }
}
