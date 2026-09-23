package com.github.h3yow.observabilitypipelinejava.storage;

import com.github.h3yow.observabilitypipelinejava.domain.AlertState;

/**
 * 单个（规则, 维度）的状态机运行时数据（可变，所有访问由 {@link AlertStateStore} 加锁）。
 */
public class AlertRuleState {

    private AlertState state;
    private int consecutiveMatches;
    private long lastEvaluatedWindowStart = Long.MIN_VALUE;
    private double lastObservedValue = Double.NaN;

    public AlertRuleState(AlertState state) {
        this.state = state;
    }

    public AlertState state() {
        return state;
    }

    public void state(AlertState state) {
        this.state = state;
    }

    /** 当前连续满足条件的窗口数。 */
    public int consecutiveMatches() {
        return consecutiveMatches;
    }

    public void consecutiveMatches(int value) {
        this.consecutiveMatches = value;
    }

    /** 最近一次评估的窗口起点。 */
    public long lastEvaluatedWindowStart() {
        return lastEvaluatedWindowStart;
    }

    public void lastEvaluatedWindowStart(long value) {
        this.lastEvaluatedWindowStart = value;
    }

    /** 最近一次观测值。 */
    public double lastObservedValue() {
        return lastObservedValue;
    }

    public void lastObservedValue(double value) {
        this.lastObservedValue = value;
    }
}
