package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 一条通知类事件（FIRED / RECOVERED）对外通知的判定结果。
 * 状态机结论不受影响，只是"是否发出去"可区分。
 */
public enum NotificationDecision {
    /** 正常对外通知。 */
    NOTIFIED,
    /** 命中静默/维护窗口，被抑制。 */
    SUPPRESSED_BY_SILENCE,
    /** 处于通知冷却期内，被抑制。 */
    SUPPRESSED_BY_COOLDOWN,
    /** 维度处于抖动降噪状态，被抑制。 */
    SUPPRESSED_BY_FLAPPING
}
