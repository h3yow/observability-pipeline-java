package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Map;

/**
 * 一条通知判定记录：输入（哪次评估、哪个生命周期事件）与结论（下发/抑制及依据）
 * 一一对应，可逐条核对。
 *
 * @param notificationId  唯一 ID（规则+维度+轮次+种类+窗口起点+序号）
 * @param kind            通知种类
 * @param decision        判定结果（下发/各类抑制）
 * @param ruleId          规则 ID
 * @param fingerprint     维度指纹
 * @param labels          维度标签
 * @param windowStartMs   触发本次判定的封存窗口起点
 * @param observedValue   该窗口观察值
 * @param fireOccurrence  告警状态机的触发轮次（恢复后再次触发递增）
 * @param notificationRound 通知轮次：每下发一次 FIRED 递增；
 *                        被抑制的重复项携带所属轮次，可与“冷却后的新一轮”区分
 * @param sourceEventId   来源生命周期事件 ID；无源事件（如冷却到期补发）时为 null
 * @param detail          判定依据（人读）
 * @param decidedAtMs     判定时刻
 */
public record NotificationRecord(String notificationId,
                                 NotificationKind kind,
                                 NotificationDecision decision,
                                 String ruleId,
                                 String fingerprint,
                                 Map<String, String> labels,
                                 long windowStartMs,
                                 double observedValue,
                                 int fireOccurrence,
                                 int notificationRound,
                                 String sourceEventId,
                                 String detail,
                                 long decidedAtMs) {
}
