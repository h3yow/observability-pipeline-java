package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Map;

/**
 * 告警规则：对某指标按聚合函数取值，与阈值做持续比较。
 *
 * @param id            规则 ID（唯一）
 * @param metric        作用指标名
 * @param labelSelector 标签选择器；样本标签须包含全部键值对。空 Map 表示该指标全部维度
 * @param function      取自窗口聚合值的哪个函数
 * @param comparator    比较符
 * @param threshold     阈值
 * @param fireAfterWindows 连续满足多少个已封存窗口后触发（&gt;=1）；
 *                         1..N-1 个连续命中期间状态为 PENDING，任一窗口不满足则清零回 INACTIVE
 */
public record AlertRule(String id,
                        String metric,
                        Map<String, String> labelSelector,
                        AggregateFunction function,
                        Comparator comparator,
                        double threshold,
                        int fireAfterWindows) {
}
