package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 单个 (规则, 维度) 抖动跟踪的可查询快照：每次判断的输入与依据可一一对上。
 */
public record FlapSnapshot(String ruleId,
                           String fingerprint,
                           boolean damping,
                           int transitionsInWindow,
                           int stableMatchedWindows,
                           int stableUnmatchedWindows,
                           long lastTransitionWindowMs,
                           long lastEvaluatedWindowMs) {
}
