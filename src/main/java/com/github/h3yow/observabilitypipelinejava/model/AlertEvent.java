package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Map;

/**
 * 告警生命周期事件。同一维度在 FIRING 期间的重复判定不会再产生 FIRED 事件；
 * 恢复（RECOVERED）后再次触发，fireOccurrence 递增、eventId 不同，可区分。 */
public record AlertEvent(String eventId,
                         AlertEventType type,
                         String ruleId,
                         String fingerprint,
                         Map<String, String> labels,
                         long windowStartMs,
                         long windowSizeMs,
                         double observedValue,
                         AggregateFunction function,
                         Comparator comparator,
                         double threshold,
                         int fireOccurrence,
                         long evaluatedAtMs) {
}
