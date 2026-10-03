package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Map;

/**
 * 通知台账记录：每条通知类判定（无论发出还是抑制）都落一条，可解释、可区分。
 *
 * @param notificationId 唯一 ID（rule:fp:occurrence:round:origin:kind:windowStart）
 * @param kind           通知类别（FIRED / RECOVERED）
 * @param origin         来源（INITIAL / REPEAT / FLAP_EXIT）
 * @param decision       判定结果（NOTIFIED / 各类 SUPPRESSED）
 * @param fireOccurrence 对应状态机的触发轮次
 * @param notifyRound    同一 occurrence 内的通知轮次：0=首次，&gt;=1=冷却后再通知
 * @param sourceEventId  来源生命周期事件 ID；REPEAT/FLAP_EXIT 无源事件时为 null
 */
public record NotificationRecord(String notificationId,
                                 String ruleId,
                                 String fingerprint,
                                 Map<String, String> labels,
                                 AlertEventType kind,
                                 NotificationOrigin origin,
                                 NotificationDecision decision,
                                 int fireOccurrence,
                                 int notifyRound,
                                 long windowStartMs,
                                 double observedValue,
                                 long decidedAtMs,
                                 String sourceEventId) {
}
