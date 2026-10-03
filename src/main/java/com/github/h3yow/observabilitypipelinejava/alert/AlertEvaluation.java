package com.github.h3yow.observabilitypipelinejava.alert;

import com.github.h3yow.observabilitypipelinejava.model.AlertState;
import java.util.Map;

/**
 * 一次「规则 × 维度 × 封存窗口」的判定输入/输出快照，
 * 作为通知控制层（静默/冷却/抖动）的判定依据，与产生的事件一一对应。
 *
 * @param ruleId         规则 ID
 * @param fingerprint    维度指纹
 * @param labels         维度标签
 * @param windowStartMs  封存窗口起点
 * @param matched        本窗口条件是否满足
 * @param observedValue  本窗口观察值
 * @param newState       本次评估后的状态机状态
 * @param fireOccurrence 评估后的触发轮次
 * @param evaluatedAtMs  评估时刻
 */
public record AlertEvaluation(String ruleId,
                              String fingerprint,
                              Map<String, String> labels,
                              long windowStartMs,
                              boolean matched,
                              double observedValue,
                              AlertState newState,
                              int fireOccurrence,
                              long evaluatedAtMs) {
}
