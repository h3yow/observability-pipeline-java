package com.github.h3yow.observabilitypipelinejava.domain;

import java.util.Map;

/**
 * 告警事件：FIRING / RESOLVED。同一规则按（ruleId + 标签集）维度去重，
 * 一次持续触发期间只产生一个 FIRING；恢复后再次触发产生新的事件序号。
 *
 * @param eventSeq    全局单调递增事件序号
 * @param ruleId      规则 ID
 * @param metric      指标名
 * @param labels      维度标签（告警维度键）
 * @param type        事件类型
 * @param windowStart 触发/恢复所依据的窗口起点
 * @param observedValue 判定时观测到的聚合值
 * @param threshold   规则阈值
 * @param reason      判定依据的人类可读说明
 * @param eventTime   事件产生时间（系统时钟，epoch 毫秒）
 */
public record AlertEvent(long eventSeq, String ruleId, String metric, Map<String, String> labels,
                         AlertEventType type, long windowStart, double observedValue, double threshold,
                         String reason, long eventTime) {
}
