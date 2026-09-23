package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 聚合窗口的生命周期状态。
 */
public enum WindowState {
    /** 打开中：仍可接收正常（非迟到）样本。 */
    OPEN,
    /** 已关闭：窗口结束时间已过，仅可在迟到宽限期内补写。 */
    CLOSED,
    /** 已封存：迟到宽限期结束，聚合值冻结、可重复复现，并参与告警判定。 */
    SEALED,
    /** 已驱逐：超出保留时长，内存数据被清理（日志/存储仍可追溯）。 */
    EVICTED
}
