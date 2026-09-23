package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 告警生命周期事件类型。恢复后再次触发会产生新的事件序号，可区分。 */
public enum AlertEventType {
    /** 进入待确认（仅用于可观测，不作为告警通知）。 */
    PENDING_STARTED,
    /** 触发。 */
    FIRED,
    /** 恢复。 */
    RECOVERED
}
