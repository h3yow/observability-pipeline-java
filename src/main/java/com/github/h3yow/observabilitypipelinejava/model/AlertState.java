package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 告警规则在单个维度（标签指纹）上的状态。
 *
 * <pre>
 *          条件连续满足 PENDING_THRESHOLD 个窗口
 *   INACTIVE ───────────────────────────────▶ PENDING
 *      ▲                                       │
 *      │ 任一窗口条件不满足（未持续）            │ 条件继续满足到 FIRE_THRESHOLD
 *      └─────────────── PENDING ───────────────┤
 *                                              ▼
 *                              FIRING ◀────────┘
 *                                 │ 条件不再满足（恢复）
 *                                 ▼
 *                              RECOVERED ──(下一个判定窗口)──▶ INACTIVE
 * </pre>
 */
public enum AlertState {
    /** 未触发：无连续满足计数。 */
    INACTIVE,
    /** 待确认：条件已开始连续满足，但尚未达到触发所需持续窗口数。 */
    PENDING,
    /** 触发：条件已持续满足达到触发阈值。 */
    FIRING,
    /** 恢复：此前 FIRING，条件已不满足；保持一个判定周期后回到 INACTIVE。 */
    RECOVERED
}
