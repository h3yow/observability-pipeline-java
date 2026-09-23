package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 降级/拒绝的可区分原因，供自观测与排障使用。 */
public enum DegradeReason {
    /** 未降级。 */
    NONE,
    /** 单指标标签序列基数超限。 */
    METRIC_CARDINALITY_EXCEEDED,
    /** 全局标签序列基数超限。 */
    GLOBAL_CARDINALITY_EXCEEDED,
    /** 上报速率超限（令牌桶）。 */
    RATE_EXCEEDED
}
