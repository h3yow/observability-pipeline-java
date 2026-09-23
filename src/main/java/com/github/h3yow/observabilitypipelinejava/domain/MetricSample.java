package com.github.h3yow.observabilitypipelinejava.domain;

import java.util.Map;

/**
 * 一条指标样本。
 *
 * @param metric    指标名
 * @param labels    维度标签（决定序列）
 * @param value     数值（必须有限）
 * @param timestamp 样本时间戳（epoch 毫秒）
 */
public record MetricSample(String metric, Map<String, String> labels, double value, long timestamp) {
}
