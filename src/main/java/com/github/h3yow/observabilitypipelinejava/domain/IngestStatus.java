package com.github.h3yow.observabilitypipelinejava.domain;

/**
 * 指标样本受理状态。
 */
public enum IngestStatus {
    /** 受理：样本落入当前可写窗口 */
    ACCEPTED,
    /** 受理（迟到）：样本属于仍在宽限期内的已封窗窗口，聚合可重算 */
    ACCEPTED_LATE,
    /** 拒绝 */
    REJECTED
}
