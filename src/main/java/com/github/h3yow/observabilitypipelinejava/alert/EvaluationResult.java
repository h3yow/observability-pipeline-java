package com.github.h3yow.observabilitypipelinejava.alert;

import com.github.h3yow.observabilitypipelinejava.model.AlertEvent;
import java.util.List;

/**
 * 一批封存窗口的判定结果：生命周期事件（状态真相）+ 每次评估的输入快照
 * （通知控制层的判定依据）。两者均按（窗口起点, 规则, 维度）排序，顺序确定。
 */
public record EvaluationResult(List<AlertEvent> events, List<AlertEvaluation> evaluations) {
}
