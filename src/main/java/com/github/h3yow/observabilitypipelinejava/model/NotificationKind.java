package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 通知记录的种类（区别于告警生命周期事件类型 AlertEventType）。
 * 生命周期事件描述“状态真相”，通知记录描述“对外通知这一次是怎么判的”。
 */
public enum NotificationKind {
    /** 触发通知（含冷却到期后的新一轮、静默/抖动退出后的补发结论）。 */
    FIRED,
    /** 恢复通知。 */
    RECOVERED,
    /** 进入待确认（仅观测，不作为告警通知下发）。 */
    PENDING,
    /** 进入抖动降噪（本身是一条可区分的通知结论）。 */
    FLAPPING_ENTERED,
    /** 退出抖动降噪（本身是一条可区分的通知结论）。 */
    FLAPPING_EXITED
}
