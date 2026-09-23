package com.github.h3yow.observabilitypipelinejava.domain;

/**
 * 告警事件类型。恢复后再次触发会产生新的 FIRING 事件（新的事件序号）。
 */
public enum AlertEventType {
    FIRING,
    RESOLVED
}
