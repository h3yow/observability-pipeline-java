package com.github.h3yow.observabilitypipelinejava.domain;

/**
 * 告警规则状态机：
 * <pre>
 *   INACTIVE --条件满足--> PENDING --连续达到阈值--> FIRING
 *      ^                       |                       |
 *      +-------条件不满足-------+-----------------------+
 *                          FIRING --条件不满足--> RESOLVED(事件) --> INACTIVE
 * </pre>
 */
public enum AlertState {
    /** 未触发 */
    INACTIVE,
    /** 待确认：条件已满足但未持续到要求的连续窗口数 */
    PENDING,
    /** 触发：已产生 FIRING 事件 */
    FIRING,
    /** 恢复：已产生 RESOLVED 事件，随后回到 INACTIVE */
    RESOLVED
}
