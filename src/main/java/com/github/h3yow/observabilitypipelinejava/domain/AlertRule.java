package com.github.h3yow.observabilitypipelinejava.domain;

import java.util.Map;

/**
 * 告警规则：对某指标按聚合方式在窗口上判定；按标签维度去重。
 *
 * @param ruleId          规则 ID
 * @param metric          目标指标
 * @param aggregation     聚合方式
 * @param op              比较算子
 * @param threshold       阈值
 * @param consecutiveFires 连续满足多少个窗口才从 PENDING 转 FIRING（&gt;=1）
 * @param labelFilter     仅对完全匹配该标签子集的序列生效；空表示不过滤
 */
public record AlertRule(String ruleId, String metric, Aggregation aggregation, ComparisonOp op,
                        double threshold, int consecutiveFires, Map<String, String> labelFilter) {
}
