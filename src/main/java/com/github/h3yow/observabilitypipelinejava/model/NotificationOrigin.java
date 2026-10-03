package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 一条通知记录的来源，用于区分"同一轮首次通知 / 冷却后新一轮通知 / 抖动退出结论"。
 */
public enum NotificationOrigin {
    /** 状态机本轮首次产生的通知（对应一次 FIRED/RECOVERED 生命周期事件）。 */
    INITIAL,
    /** 冷却期结束后维度仍在 FIRING，补发的新一轮通知。 */
    REPEAT,
    /** 抖动降噪退出时给出的一次性结论。 */
    FLAP_EXIT
}
