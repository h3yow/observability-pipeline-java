package com.github.h3yow.observabilitypipelinejava.aggregate;

import com.github.h3yow.observabilitypipelinejava.model.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.storage.StoredSample;
import com.github.h3yow.observabilitypipelinejava.time.WindowTiming;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 窗口聚合器：维护 (序列键, 窗口起点) -> {@link WindowAggregate}。
 *
 * <p>并发写入通过 {@link ConcurrentHashMap#computeIfAbsent} 保证同一序列同一窗口
 * 只有一个累积器实例；累积器内部 synchronized，保证计数精确。
 *
 * <p>生命周期：
 * <ol>
 *   <li>样本到达时惰性创建窗口（未封存窗口在迟到宽限期内仍可补写）；</li>
 *   <li>{@code end + lateness <= now} 的窗口被封存，冻结并交给告警判定；</li>
 *   <li>{@code end + retention <= now} 的窗口被驱逐，释放内存与基数配额。</li>
 * </ol>
 */
public class WindowAggregator {

    /** 写入结果三态。 */
    public enum ApplyResult { APPLIED, DUPLICATE, REJECTED_SEALED }

    /** 被彻底驱逐（不再有任何窗口）的序列引用，用于释放基数配额。 */
    public record EvictedSeries(String metric, String fingerprint, String seriesKey) {
    }

    /** 驱逐结果。 */
    public record EvictionResult(int windowsEvicted, List<EvictedSeries> evictedSeries) {
    }

    private final WindowTiming timing;
    private final Map<String, WindowAggregate> windows = new ConcurrentHashMap<>();

    public WindowAggregator(WindowTiming timing) {
        this.timing = timing;
    }

    private static String entryKey(String seriesKey, long windowStartMs) {
        return seriesKey + '#' + windowStartMs;
    }

    /** 不写入的前提下探测是否为窗口内重复；窗口不存在或已封存返回 false。 */
    public boolean isDuplicate(StoredSample sample) {
        long start = timing.windowStartFor(sample.timestampMs());
        String seriesKey = sample.metric() + '|' + sample.fingerprint();
        WindowAggregate agg = windows.get(entryKey(seriesKey, start));
        return agg != null && agg.isDuplicate(sample);
    }

    /** 写入一条已受理样本。 */
    public ApplyResult apply(StoredSample sample) {
        long start = timing.windowStartFor(sample.timestampMs());
        String seriesKey = sample.metric() + '|' + sample.fingerprint();
        WindowAggregate agg = windows.computeIfAbsent(entryKey(seriesKey, start), k ->
                new WindowAggregate(sample.metric(), sample.labels(), sample.fingerprint(),
                        seriesKey, start, timing.windowSizeMs()));
        if (agg.apply(sample)) {
            return ApplyResult.APPLIED;
        }
        return agg.isSealed() ? ApplyResult.REJECTED_SEALED : ApplyResult.DUPLICATE;
    }

    /**
     * 封存所有满足 {@code end + lateness <= now} 的未封存窗口。
     * 返回结果按（窗口起点, 序列键）排序，保证多线程后的判定顺序仍然确定。
     */
    public List<WindowAggregate> sealDueWindows(long nowMs) {
        long horizon = timing.sealableHorizonStart(nowMs);
        List<WindowAggregate> sealed = new ArrayList<>();
        for (WindowAggregate agg : windows.values()) {
            if (!agg.isSealed() && agg.getWindowStartMs() <= horizon) {
                agg.seal(nowMs);
                sealed.add(agg);
            }
        }
        sealed.sort(Comparator.comparingLong(WindowAggregate::getWindowStartMs)
                .thenComparing(WindowAggregate::getSeriesKey));
        return sealed;
    }

    /**
     * 驱逐 {@code end + retention <= now} 的窗口；
     * 若某序列已无任何窗口，一并返回以便基数守卫释放配额。
     */
    public EvictionResult evictExpired(long nowMs) {
        long horizon = timing.evictHorizonStart(nowMs);
        List<WindowAggregate> removed = new ArrayList<>();
        for (var entry : windows.entrySet()) {
            WindowAggregate agg = entry.getValue();
            if (agg.getWindowStartMs() <= horizon && windows.remove(entry.getKey(), agg)) {
                removed.add(agg);
            }
        }
        // 驱逐后仍有窗口的序列不释放配额。
        java.util.Set<String> stillAlive = new java.util.HashSet<>();
        for (WindowAggregate agg : windows.values()) {
            stillAlive.add(agg.getSeriesKey());
        }
        List<EvictedSeries> deadSeries = new ArrayList<>();
        java.util.Set<String> reported = new java.util.HashSet<>();
        for (WindowAggregate agg : removed) {
            if (!stillAlive.contains(agg.getSeriesKey()) && reported.add(agg.getSeriesKey())) {
                deadSeries.add(new EvictedSeries(agg.getMetric(), agg.getFingerprint(),
                        agg.getSeriesKey()));
            }
        }
        removed.sort(Comparator.comparingLong(WindowAggregate::getWindowStartMs)
                .thenComparing(WindowAggregate::getSeriesKey));
        deadSeries.sort(Comparator.comparing(EvictedSeries::seriesKey));
        return new EvictionResult(removed.size(), deadSeries);
    }

    /** 查询某指标某序列在指定窗口的聚合（不存在返回 null）。 */
    public WindowAggregate query(String metric, String fingerprint, long windowStartMs) {
        return windows.get(entryKey(metric + '|' + fingerprint, windowStartMs));
    }

    /** 查询某指标在指定窗口的全部序列聚合，按序列键排序。 */
    public List<WindowAggregate> queryByMetric(String metric, long windowStartMs) {
        List<WindowAggregate> out = new ArrayList<>();
        for (WindowAggregate agg : windows.values()) {
            if (agg.getMetric().equals(metric) && agg.getWindowStartMs() == windowStartMs) {
                out.add(agg);
            }
        }
        out.sort(Comparator.comparing(WindowAggregate::getSeriesKey));
        return out;
    }

    /** 当前窗口条目数（含未封存与已封存，驱逐后归零）。 */
    public int windowEntryCount() {
        return windows.size();
    }
}
