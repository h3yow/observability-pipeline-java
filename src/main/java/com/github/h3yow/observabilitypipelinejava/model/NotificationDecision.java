package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 单条通知记录的判定结果。所有被抑制的情况都有独立取值，
 * 不会出现“默默生效/默默失效”。
 */
public enum NotificationDecision {
    /** 正常下发。 */
    DELIVERED,
    /** 通知跟踪器已达容量上限，无法纳入冷却/抖动跟踪，降级为直接下发（可区分）。 */
    DELIVERED_UNTRACKED,
    /** 命中静默/维护窗口，本次不下发。 */
    SUPPRESSED_SILENCE,
    /** 处于通知冷却期内的重复触发，本次不下发。 */
    SUPPRESSED_COOLDOWN,
    /** 处于抖动降噪期间，本次不下发。 */
    SUPPRESSED_FLAPPING,
    /** 对应的触发通知此前被抑制，恢复通知不下发（避免凭空冒出恢复）。 */
    SUPPRESSED_ORPHAN_RECOVERY,
    /** 非通知类记录（如 PENDING 观测），不下发。 */
    OBSERVATION
}
