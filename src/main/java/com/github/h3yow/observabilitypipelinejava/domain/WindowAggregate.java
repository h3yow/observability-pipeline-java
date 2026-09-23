package com.github.h3yow.observabilitypipelinejava.domain;

import java.util.Map;

/**
 * 一个（指标, 标签集, 窗口）的聚合结果。聚合仅依赖窗口内样本集合，可重复复现；
 * 迟到样本并入后以同一确定性算法重算。
 *
 * @param metric        指标名
 * @param labels        维度标签
 * @param windowStart   窗口起点（epoch 毫秒）
 * @param windowEnd     窗口结束（不含）
 * @param count         样本数（去重后）
 * @param sum           样本值之和
 * @param min           最小值
 * @param max           最大值
 * @param avg           平均值
 * @param lastTimestamp 窗口内最大样本时间戳
 * @param sealed        窗口是否已封（不再接受非迟到更新）
 */
public record WindowAggregate(String metric, Map<String, String> labels, long windowStart, long windowEnd,
                              long count, double sum, double min, double max, double avg,
                              long lastTimestamp, boolean sealed) {
}
