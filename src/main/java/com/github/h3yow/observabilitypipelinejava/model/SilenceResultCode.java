package com.github.h3yow.observabilitypipelinejava.model;

/** 静默窗口设置/取消的结果码：每种异常情形都有独立取值，绝不默默生效或失效。 */
public enum SilenceResultCode {
    /** 创建成功。 */
    CREATED,
    /** 相同 ID 的静默已存在（重复设置）。 */
    REJECTED_DUPLICATE,
    /** 与已有静默在同一规则+维度范围上时间重叠。 */
    REJECTED_OVERLAP,
    /** 时间区间非法（endMs <= startMs 或边界为负）。 */
    REJECTED_INVALID_RANGE,
    /** 区间整体已过去（endMs <= now），设置无意义。 */
    REJECTED_PAST,
    /** 静默数量达到可控上限。 */
    REJECTED_CAPACITY,
    /** 取消成功。 */
    CANCELLED,
    /** 取消目标不存在。 */
    NOT_FOUND
}
