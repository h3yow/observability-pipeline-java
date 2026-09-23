package com.github.h3yow.observabilitypipelinejava.storage;

import com.github.h3yow.observabilitypipelinejava.window.SeriesKey;

/**
 * 规则维度键：同一规则按规则 ID + 标签维度独立维护状态（维度去重）。
 */
public record RuleStateKey(String ruleId, SeriesKey series) {
}
