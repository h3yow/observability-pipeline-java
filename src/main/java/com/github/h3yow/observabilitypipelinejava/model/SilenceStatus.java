package com.github.h3yow.observabilitypipelinejava.model;

/** 静默窗口在某个时刻的状态（由时间区间计算得出，不持久化）。 */
public enum SilenceStatus {
    /** 尚未开始。 */
    SCHEDULED,
    /** 生效中。 */
    ACTIVE,
    /** 已结束。 */
    EXPIRED
}
