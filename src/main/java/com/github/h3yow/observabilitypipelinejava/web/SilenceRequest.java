package com.github.h3yow.observabilitypipelinejava.web;

import java.util.Map;

/**
 * 设置静默/维护窗口的请求体。
 *
 * @param ruleId        目标规则（必填）
 * @param labelSelector 维度选择器；空/缺省表示该规则全部维度
 * @param startMs       区间起点（含）
 * @param endMs         区间终点（不含）
 * @param reason        备注（可选）
 */
public record SilenceRequest(String ruleId,
                             Map<String, String> labelSelector,
                             Long startMs,
                             Long endMs,
                             String reason) {
}
