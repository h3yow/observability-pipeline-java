package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Map;

/**
 * 一条指标样本（一次上报的输入）。
 *
 * @param metric      指标名，非空
 * @param labels      标签维度（可为空 Map），决定序列指纹
 * @param timestampMs 样本时间戳（epoch millis）
 * @param value       样本数值（不允许 NaN / Infinity）
 * @param sourceId    上报来源标识，用于多来源区分与重复判定
 */
public record MetricSample(String metric,
                           Map<String, String> labels,
                           Long timestampMs,
                           double value,
                           String sourceId) {
}
