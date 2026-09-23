package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 单条指标样本上报后的受理结果。
 */
public enum IngestStatus {
    /** 受理：写入当前打开的窗口。 */
    ACCEPTED,
    /** 受理：迟到样本，已补入已关闭但仍在迟到宽限期内的窗口。 */
    ACCEPTED_LATE,
    /** 拒绝：与窗口内已有样本完全重复（指标+标签+时间戳+数值+来源）。 */
    DUPLICATE,
    /** 拒绝：时间戳为空、为负或超出可接受范围。 */
    INVALID_TIMESTAMP,
    /** 拒绝：指标名为空或非法。 */
    INVALID_METRIC,
    /** 拒绝：标签键非法或标签值数量/大小超限。 */
    INVALID_LABELS,
    /** 拒绝：样本数值非法（NaN / Infinity）。 */
    INVALID_VALUE,
    /** 拒绝：时间戳早于允许的最早迟到边界，窗口已不可补写。 */
    TOO_OLD,
    /** 拒绝：时间戳远在未来，超过可接受的未来偏差。 */
    TOO_FAR_FUTURE,
    /** 拒绝：该指标的标签基数已达上限。 */
    CARDINALITY_LIMIT,
    /** 拒绝：全局序列基数已达上限。 */
    GLOBAL_CARDINALITY_LIMIT,
    /** 拒绝：上报速率超过令牌桶配额。 */
    RATE_LIMITED
}
