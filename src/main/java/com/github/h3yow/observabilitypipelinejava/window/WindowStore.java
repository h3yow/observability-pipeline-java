package com.github.h3yow.observabilitypipelinejava.window;

import com.github.h3yow.observabilitypipelinejava.config.PipelineProperties;
import com.github.h3yow.observabilitypipelinejava.domain.WindowAggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 窗口数据的并发内存存储：
 * <ul>
 *   <li>按 序列键 -> (窗口起点 -> bucket) 组织；</li>
 *   <li>同窗口同 (时间戳,值) 去重；</li>
 *   <li>基数按"当前仍在保留期内出现过的序列"计量，窗口过期后释放计数；</li>
 *   <li>所有读写均线程安全。</li>
 * </ul>
 */
public class WindowStore {

    private static final class SeriesEntry {
        final ConcurrentHashMap<Long, WindowBucket> buckets = new ConcurrentHashMap<>();
    }

    private final PipelineProperties props;
    private final ConcurrentHashMap<SeriesKey, SeriesEntry> series = new ConcurrentHashMap<>();
    /** 指标名 -> 该指标活跃序列数（保留期内出现过即计入，回收时减少） */
    private final ConcurrentHashMap<String, Integer> perMetricSeries = new ConcurrentHashMap<>();

    public WindowStore(PipelineProperties props) {
        this.props = props;
    }

    /**
     * 写入一个样本。新序列在落桶前先做基数判定，超限拒绝而不创建桶：
     * 内存不会因高基数标签而无限增长。
     */
    public AddOutcome addSample(SeriesKey key, long windowStart, long timestampMillis, double value) {
        // 已存在的序列直接走落桶逻辑
        SeriesEntry existing = series.get(key);
        if (existing == null) {
            // 新序列：在全局锁段内做基数判定 + 注册，避免并发下双超限竞态
            synchronized (this) {
                if (!series.containsKey(key)) {
                    int globalCount = series.size();
                    if (globalCount >= props.getMaxSeriesGlobal()) {
                        return AddOutcome.GLOBAL_LIMIT;
                    }
                    int metricCount = perMetricSeries.getOrDefault(key.metric(), 0);
                    if (metricCount >= props.getMaxSeriesPerMetric()) {
                        return AddOutcome.SERIES_LIMIT;
                    }
                    series.put(key, new SeriesEntry());
                    perMetricSeries.merge(key.metric(), 1, Integer::sum);
                }
            }
        }

        WindowBucket bucket = series.get(key).buckets.computeIfAbsent(windowStart,
                ws -> new WindowBucket(key, ws));
        synchronized (bucket) {
            return bucket.add(value, timestampMillis) ? AddOutcome.ADDED : AddOutcome.DUPLICATE;
        }
    }

    /** 该序列已受理样本中的最大时间戳（用于乱序判定）；无数据返回 Long.MIN_VALUE。 */
    public long maxTimestampSeen(SeriesKey key) {
        SeriesEntry entry = series.get(key);
        if (entry == null) {
            return Long.MIN_VALUE;
        }
        long max = Long.MIN_VALUE;
        for (WindowBucket bucket : entry.buckets.values()) {
            max = Math.max(max, bucket.lastTimestamp());
        }
        return max;
    }

    /** 查询某指标在指定窗口、全部序列的聚合。 */
    public List<WindowAggregate> aggregatesForWindow(String metric, long windowStart, long windowSizeMillis) {
        List<WindowAggregate> result = new ArrayList<>();
        for (Map.Entry<SeriesKey, SeriesEntry> e : series.entrySet()) {
            if (!e.getKey().metric().equals(metric)) {
                continue;
            }
            WindowBucket bucket = e.getValue().buckets.get(windowStart);
            if (bucket != null && bucket.sampleCount() > 0) {
                result.add(bucket.aggregate(windowSizeMillis));
            }
        }
        // 按维度字符串排序，保证下游评估顺序确定（事件顺序可复现）
        result.sort(ComparatorHolder.INSTANCE);
        return result;
    }

    /** 排序器持有类（避免记录类的泛型数组问题）。 */
    private static final class ComparatorHolder {
        static final java.util.Comparator<WindowAggregate> INSTANCE =
                java.util.Comparator.comparing(a -> a.labels().toString());
    }

    /** 当前（保留期内）活跃序列总数。 */
    public int activeSeriesCount() {
        return series.size();
    }

    /** 当前某指标（保留期内）活跃序列数。 */
    public int activeSeriesCountForMetric(String metric) {
        return perMetricSeries.getOrDefault(metric, 0);
    }

    /** 全部活跃指标名（只读快照）。 */
    public Set<String> activeMetrics() {
        return Set.copyOf(perMetricSeries.keySet());
    }

    /**
     * 回收早于 oldestToKeepWindowStart 的窗口，并更新序列基数计数；
     * 当一个序列的全部窗口都被回收时，注销该序列并释放基数计数。
     * 返回被清理的样本总数（用于观测）。
     */
    public int evictBefore(long oldestToKeepWindowStart) {
        int removedSamples = 0;
        List<SeriesKey> emptyKeys = new ArrayList<>();
        for (Map.Entry<SeriesKey, SeriesEntry> e : series.entrySet()) {
            SeriesEntry entry = e.getValue();
            synchronized (entry) {
                var iter = entry.buckets.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Long, WindowBucket> b = iter.next();
                    if (b.getKey() < oldestToKeepWindowStart) {
                        removedSamples += b.getValue().sampleCount();
                        iter.remove();
                    }
                }
                if (entry.buckets.isEmpty()) {
                    emptyKeys.add(e.getKey());
                }
            }
        }
        if (!emptyKeys.isEmpty()) {
            synchronized (this) {
                for (SeriesKey key : emptyKeys) {
                    if (series.remove(key) != null) {
                        perMetricSeries.computeIfPresent(key.metric(), (m, c) -> c <= 1 ? null : c - 1);
                    }
                }
            }
        }
        return removedSamples;
    }
}
