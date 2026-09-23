package com.github.h3yow.observabilitypipelinejava.domain;

/**
 * 样本被拒绝的可区分原因。
 */
public enum RejectReason {
    /** 未拒绝 */
    NONE,
    /** 时间戳为空 */
    NULL_TIMESTAMP,
    /** 时间戳过早（早于保留期下界） */
    TIMESTAMP_TOO_OLD,
    /** 窗口已封板且超出宽限期，迟到样本不再受理 */
    WINDOW_CLOSED,
    /** 时间戳过晚（超过允许的未来钟偏） */
    TIMESTAMP_IN_FUTURE,
    /** 指标名为空 */
    EMPTY_METRIC_NAME,
    /** 数值非有限值（NaN/Infinity） */
    NON_FINITE_VALUE,
    /** 标签键为空 */
    EMPTY_LABEL_KEY,
    /** 同一（序列,窗口,纳秒时间,值）重复上报 */
    DUPLICATE,
    /** 该指标的序列基数超限（降级） */
    CARDINALITY_LIMIT,
    /** 全局序列基数超限（降级） */
    GLOBAL_CARDINALITY_LIMIT,
    /** 上报速率超限（降级） */
    RATE_LIMITED
}
