package com.github.h3yow.observabilitypipelinejava.guard;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.DegradeReason;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 标签基数守卫：单指标与全局序列数超限则拒绝新序列。
 *
 * <p>语义：
 * <ul>
 *   <li>已注册序列永远放行（同序列重复上报不受限）；</li>
 *   <li>新序列先查单指标上限，再查全局上限，原子接纳并注册，并发下不会超发；</li>
 *   <li>窗口驱逐后由聚合器回调 {@link #deregister}，配额回落，系统恢复接纳；</li>
 *   <li>拒绝原因可区分：{@link DegradeReason#METRIC_CARDINALITY_EXCEEDED} /
 *       {@link DegradeReason#GLOBAL_CARDINALITY_EXCEEDED}。</li>
 * </ul>
 */
public class CardinalityGuard {

    public record Decision(boolean allowed, DegradeReason reason, String detail) {
        public static Decision ok() {
            return new Decision(true, DegradeReason.NONE, "series already registered or within quota");
        }
    }

    private final int maxPerMetric;
    private final int maxGlobal;
    private final Map<String, Set<String>> perMetric = new HashMap<>();
    private final Set<String> global = new HashSet<>();

    public CardinalityGuard(ObservabilityProperties properties) {
        this.maxPerMetric = properties.getMaxSeriesPerMetric();
        this.maxGlobal = properties.getMaxGlobalSeries();
    }

    /**
     * 原子判定并注册一个（可能新的）序列。
     * 已注册 -> 放行；新序列且超限 -> 拒绝且不注册；新序列未超限 -> 注册并放行。
     */
    public synchronized Decision admitAndRegister(String metric, String fingerprint) {
        Set<String> owned = perMetric.get(metric);
        if (owned != null && owned.contains(fingerprint)) {
            return Decision.ok();
        }
        if (owned != null && owned.size() >= maxPerMetric) {
            return new Decision(false, DegradeReason.METRIC_CARDINALITY_EXCEEDED,
                    "metric '" + metric + "' series cardinality " + owned.size()
                            + " >= limit " + maxPerMetric);
        }
        if (global.size() >= maxGlobal) {
            return new Decision(false, DegradeReason.GLOBAL_CARDINALITY_EXCEEDED,
                    "global series cardinality " + global.size() + " >= limit " + maxGlobal);
        }
        perMetric.computeIfAbsent(metric, m -> new HashSet<>()).add(fingerprint);
        global.add(metric + '|' + fingerprint);
        return Decision.ok();
    }

    /** 注销序列（窗口驱逐后配额回落）。 */
    public synchronized void deregister(String metric, String fingerprint) {
        Set<String> owned = perMetric.get(metric);
        if (owned != null) {
            owned.remove(fingerprint);
            if (owned.isEmpty()) {
                perMetric.remove(metric);
            }
        }
        global.remove(metric + '|' + fingerprint);
    }

    public synchronized int currentGlobalCount() {
        return global.size();
    }

    public synchronized Map<String, Integer> currentPerMetric() {
        Map<String, Integer> snapshot = new HashMap<>();
        perMetric.forEach((m, set) -> snapshot.put(m, set.size()));
        return Collections.unmodifiableMap(snapshot);
    }

    public synchronized boolean isMetricLimited(String metric) {
        Set<String> owned = perMetric.get(metric);
        return owned != null && owned.size() >= maxPerMetric;
    }

    public synchronized boolean isGlobalLimited() {
        return global.size() >= maxGlobal;
    }
}
