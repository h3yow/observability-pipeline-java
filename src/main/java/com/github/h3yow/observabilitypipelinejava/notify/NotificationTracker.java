package com.github.h3yow.observabilitypipelinejava.notify;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

/**
 * 单个「规则 × 维度」的通知跟踪状态（可变，仅在 NotificationGate 的同步方法内访问）。
 * 跟踪：已下发的触发/恢复进度、冷却计时、抖动转移历史与稳定计数。
 */
final class NotificationTracker {

    final String ruleId;
    final String fingerprint;
    Map<String, String> labels;

    /** 已下发 FIRED 的状态机轮次（0 = 尚未下发过）。 */
    int deliveredFireOccurrence;
    /** 是否存在已下发但尚未下发对应恢复的 FIRED。 */
    boolean pendingDeliveredFire;
    /** 最近一次下发 FIRED 的时刻（冷却计时基准）。 */
    long lastFireNotifiedAtMs = Long.MIN_VALUE;
    /** 通知轮次：每下发一次 FIRED 递增；被抑制的重复项携带所属轮次。 */
    int notificationRound;

    /** 观察窗口内的状态转移（FIRED/RECOVERED 生命周期事件）时刻。 */
    final Deque<Long> transitionTimes = new ArrayDeque<>();
    /** 是否处于抖动降噪中。 */
    boolean flapping;
    /** 降噪期间连续满足/不满足的窗口数。 */
    int stableMatched;
    int stableUnmatched;

    long lastWindowStartMs = Long.MIN_VALUE;
    long updatedAtMs;

    NotificationTracker(String ruleId, String fingerprint, Map<String, String> labels) {
        this.ruleId = ruleId;
        this.fingerprint = fingerprint;
        this.labels = labels;
    }
}
