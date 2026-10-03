package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 设置静默区间的结果，各种异常情形明确可区分，不会默默生效或失效。
 */
public enum SilenceCreateStatus {
    /** 创建成功。 */
    CREATED,
    /** 时间区间非法（endMs &lt;= startMs）。 */
    INVALID_RANGE,
    /** 区间整体落在过去（endMs &lt;= now），设置无意义。 */
    IN_PAST,
    /** 完全相同的静默（同规则、同选择器、同区间）已存在。 */
    DUPLICATE,
    /** 与同规则、同选择器的现有静默区间重叠。 */
    OVERLAP,
    /** 静默条目达到可控上限，拒绝新增。 */
    LIMIT_REACHED
}
