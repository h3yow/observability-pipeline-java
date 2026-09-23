package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Map;

/**
 * 规则在单个维度上的状态快照（可解释判定依据）。 */
public record AlertStateSnapshot(String ruleId,
                                 String fingerprint,
                                 Map<String, String> labels,
                                 AlertState state,
                                 int consecutiveMatchedWindows,
                                 double lastObservedValue,
                                 long lastWindowStartMs,
                                 int fireOccurrence,
                                 long updatedAtMs) {
}
