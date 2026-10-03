package com.github.h3yow.observabilitypipelinejava.notify;

import com.github.h3yow.observabilitypipelinejava.alert.AlertEvaluation;
import com.github.h3yow.observabilitypipelinejava.alert.AlertRuleRegistry;
import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.model.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.model.AlertState;
import com.github.h3yow.observabilitypipelinejava.model.NotificationDecision;
import com.github.h3yow.observabilitypipelinejava.model.NotificationKind;
import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 通知控制层：叠加在告警状态机之上，只决定“对外通知”，不改写状态真相。
 *
 * <p>每条评估输入（{@link AlertEvaluation}）与其产生的生命周期事件一一对应，
 * 输出 {@link NotificationRecord}：下发（DELIVERED）或带原因的抑制
 * （静默/冷却/抖动/孤儿恢复）。语义：
 * <ul>
 *   <li><b>静默</b>：命中静默窗口的评估不产生通知；静默结束后按当前真实状态
 *       补一次结论（仍触发→补发一次 FIRED；已恢复→不凭空补恢复，除非对应
 *       触发此前真的下发过）；</li>
 *   <li><b>冷却</b>：下发 FIRED 后 cooldownMs 内不再下发新的触发通知；
 *       冷却结束仍触发则再发一次（notificationRound 递增，可区分）；</li>
 *   <li><b>抖动</b>：观察窗口内状态转移次数达阈值进入降噪，期间不逐条通知；
 *       连续稳定满足/不满足达设定次数后退出并给出一次结论；</li>
 *   <li><b>一致性</b>：同一维度下发的通知流保持 FIRED/RECOVERED 交替，
 *       恢复只在对应触发确实下发过之后才下发；</li>
 *   <li><b>容量</b>：跟踪器数量受控，超限时不崩溃，降级为
 *       {@code DELIVERED_UNTRACKED}（可区分）；规则删除/维度老化后状态被回收。</li>
 * </ul>
 * 同一批输入按（窗口, 规则, 维度）顺序处理，处理过程同步，结论可复现。
 */
public class NotificationGate {

    /** 跟踪器只读快照（供查询与测试核对）。 */
    public record TrackerSnapshot(String ruleId, String fingerprint, boolean flapping,
                                  int notificationRound, int deliveredFireOccurrence,
                                  boolean pendingDeliveredFire, int trackedTransitions,
                                  long lastFireNotifiedAtMs, long updatedAtMs) {
    }

    private final SilenceRegistry silenceRegistry;
    private final AlertRuleRegistry ruleRegistry;
    private final long cooldownMs;
    private final long flapWindowMs;
    private final int flapTransitionThreshold;
    private final int flapExitStableWindows;
    private final int maxTrackers;
    private final long trackerIdleMs;

    private final Map<String, NotificationTracker> trackers = new LinkedHashMap<>();
    private long recordSeq;

    public NotificationGate(SilenceRegistry silenceRegistry, AlertRuleRegistry ruleRegistry,
                            ObservabilityProperties props) {
        this.silenceRegistry = silenceRegistry;
        this.ruleRegistry = ruleRegistry;
        this.cooldownMs = props.getNotificationCooldownMs();
        this.flapWindowMs = props.getNotificationFlapWindowMs();
        this.flapTransitionThreshold = props.getNotificationFlapTransitionThreshold();
        this.flapExitStableWindows = props.getNotificationFlapExitStableWindows();
        this.maxTrackers = props.getNotificationMaxTrackers();
        this.trackerIdleMs = props.getNotificationTrackerIdleMs();
    }

    private boolean flappingEnabled() {
        return flapWindowMs > 0 && flapTransitionThreshold >= 2 && flapExitStableWindows >= 1;
    }

    /**
     * 处理一批评估（顺序确定），返回本次产生的通知判定记录。
     * 与上报并发安全：本方法同步，且仅由推进线程调用。
     */
    public synchronized List<NotificationRecord> process(List<AlertEvaluation> evaluations,
                                                         List<AlertEvent> events, long nowMs) {
        Map<String, List<AlertEvent>> eventsByEval = new HashMap<>();
        for (AlertEvent e : events) {
            eventsByEval.computeIfAbsent(
                    e.ruleId() + "|" + e.fingerprint() + "|" + e.windowStartMs(),
                    k -> new ArrayList<>()).add(e);
        }
        evictTrackers(nowMs);
        List<NotificationRecord> out = new ArrayList<>();
        for (AlertEvaluation ev : evaluations) {
            List<AlertEvent> evEvents = eventsByEval.getOrDefault(
                    ev.ruleId() + "|" + ev.fingerprint() + "|" + ev.windowStartMs(), List.of());
            processOne(ev, evEvents, out, nowMs);
        }
        return out;
    }

    private void processOne(AlertEvaluation ev, List<AlertEvent> events,
                            List<NotificationRecord> out, long nowMs) {
        NotificationTracker t = trackerFor(ev, nowMs);
        if (t == null) {
            // 跟踪器容量超限：不崩溃、不静默，降级为可区分的直接下发。
            for (AlertEvent e : events) {
                switch (e.type()) {
                    case FIRED -> out.add(record(NotificationKind.FIRED,
                            NotificationDecision.DELIVERED_UNTRACKED, ev, e, 0,
                            "tracker capacity reached (" + maxTrackers + "), delivered untracked",
                            nowMs));
                    case RECOVERED -> out.add(record(NotificationKind.RECOVERED,
                            NotificationDecision.DELIVERED_UNTRACKED, ev, e, 0,
                            "tracker capacity reached (" + maxTrackers + "), delivered untracked",
                            nowMs));
                    case PENDING_STARTED -> out.add(record(NotificationKind.PENDING,
                            NotificationDecision.OBSERVATION, ev, e, 0,
                            "pending observation (untracked)", nowMs));
                }
            }
            return;
        }
        t.labels = ev.labels();
        t.lastWindowStartMs = ev.windowStartMs();
        t.updatedAtMs = nowMs;

        // 1) 抖动转移跟踪：基于状态真相（生命周期事件），与是否静默无关。
        for (AlertEvent e : events) {
            if (e.type() == AlertEventType.FIRED || e.type() == AlertEventType.RECOVERED) {
                t.transitionTimes.addLast(nowMs);
            }
        }
        while (!t.transitionTimes.isEmpty()
                && t.transitionTimes.peekFirst() < nowMs - flapWindowMs) {
            t.transitionTimes.pollFirst();
        }
        boolean silenced = silenceRegistry.isSilenced(ev.ruleId(), ev.fingerprint(), nowMs);
        boolean justEnteredFlapping = false;
        if (!t.flapping && flappingEnabled()
                && t.transitionTimes.size() >= flapTransitionThreshold) {
            t.flapping = true;
            t.stableMatched = 0;
            t.stableUnmatched = 0;
            justEnteredFlapping = true;
            out.add(record(NotificationKind.FLAPPING_ENTERED,
                    silenced ? NotificationDecision.SUPPRESSED_SILENCE
                            : NotificationDecision.DELIVERED,
                    ev, null, t.notificationRound,
                    "transitions=" + t.transitionTimes.size() + " within flapWindowMs="
                            + flapWindowMs + " >= threshold=" + flapTransitionThreshold,
                    nowMs));
        }

        // 2) 降噪期间：不逐条通知，累计稳定计数，达到退出条件后给出一次结论。
        if (t.flapping) {
            for (AlertEvent e : events) {
                switch (e.type()) {
                    case FIRED -> out.add(record(NotificationKind.FIRED,
                            NotificationDecision.SUPPRESSED_FLAPPING, ev, e, t.notificationRound,
                            "flapping: fire suppressed", nowMs));
                    case RECOVERED -> out.add(record(NotificationKind.RECOVERED,
                            NotificationDecision.SUPPRESSED_FLAPPING, ev, e, t.notificationRound,
                            "flapping: recovery suppressed", nowMs));
                    case PENDING_STARTED -> out.add(record(NotificationKind.PENDING,
                            NotificationDecision.OBSERVATION, ev, e, t.notificationRound,
                            "pending observation", nowMs));
                }
            }
            if (justEnteredFlapping) {
                // 进入降噪的窗口本身是一次转移，不计入“连续稳定”。
                return;
            }
            if (ev.matched()) {
                t.stableMatched++;
                t.stableUnmatched = 0;
            } else {
                t.stableUnmatched++;
                t.stableMatched = 0;
            }
            if (t.stableMatched >= flapExitStableWindows
                    || t.stableUnmatched >= flapExitStableWindows) {
                t.flapping = false;
                t.transitionTimes.clear();
                out.add(record(NotificationKind.FLAPPING_EXITED,
                        silenced ? NotificationDecision.SUPPRESSED_SILENCE
                                : NotificationDecision.DELIVERED,
                        ev, null, t.notificationRound,
                        "exit after stableMatched=" + t.stableMatched + " stableUnmatched="
                                + t.stableUnmatched + " (required=" + flapExitStableWindows + ")",
                        nowMs));
                t.stableMatched = 0;
                t.stableUnmatched = 0;
                if (!silenced) {
                    reconcile(t, ev, out, nowMs, "flap-exit");
                }
            }
            return;
        }

        // 3) 正常路径：逐事件判定（静默 > 冷却 > 下发）。
        for (AlertEvent e : events) {
            switch (e.type()) {
                case PENDING_STARTED -> out.add(record(NotificationKind.PENDING,
                        NotificationDecision.OBSERVATION, ev, e, t.notificationRound,
                        "pending observation", nowMs));
                case FIRED -> {
                    if (silenced) {
                        out.add(record(NotificationKind.FIRED,
                                NotificationDecision.SUPPRESSED_SILENCE, ev, e,
                                t.notificationRound, "silenced at " + nowMs, nowMs));
                    } else if (cooldownActive(t, nowMs)) {
                        out.add(record(NotificationKind.FIRED,
                                NotificationDecision.SUPPRESSED_COOLDOWN, ev, e,
                                t.notificationRound,
                                "cooldown " + cooldownMs + "ms since last fire notification at "
                                        + t.lastFireNotifiedAtMs, nowMs));
                    } else {
                        deliverFire(t, ev, e, out, nowMs, "new fire");
                    }
                }
                case RECOVERED -> {
                    if (silenced) {
                        out.add(record(NotificationKind.RECOVERED,
                                NotificationDecision.SUPPRESSED_SILENCE, ev, e,
                                t.notificationRound, "silenced at " + nowMs, nowMs));
                    } else if (t.pendingDeliveredFire
                            && t.deliveredFireOccurrence == e.fireOccurrence()) {
                        deliverRecovery(t, ev, e, out, nowMs, "recovery of delivered fire");
                    } else {
                        out.add(record(NotificationKind.RECOVERED,
                                NotificationDecision.SUPPRESSED_ORPHAN_RECOVERY, ev, e,
                                t.notificationRound,
                                "fire occurrence " + e.fireOccurrence()
                                        + " was not notified; suppressing phantom recovery",
                                nowMs));
                    }
                }
            }
        }

        // 4) 对账：仅当本次评估没有产生生命周期事件时才需要——
        // 有事件时事件路径已完整决定跟踪器状态，再对账只会重复记录。
        boolean hadLifecycleEvent = events.stream().anyMatch(e ->
                e.type() == AlertEventType.FIRED || e.type() == AlertEventType.RECOVERED);
        if (!silenced && !hadLifecycleEvent) {
            reconcile(t, ev, out, nowMs, "eval");
        }
    }

    /**
     * 对账：静默/冷却/抖动结束后，让通知流与真实状态一致——
     * 已下发的触发与当前真实轮次不符 → 先补一次恢复结清旧账；
     * 仍触发且本轮未通知过 → 补发一次 FIRED；冷却到期仍在触发 → 再发一轮 FIRED。
     * 不会凭空造恢复（以 pendingDeliveredFire 为准），也不会把期间憋着的
     * 命中补发成一串通知（每个评估周期至多一次结论）。
     */
    private void reconcile(NotificationTracker t, AlertEvaluation ev,
                           List<NotificationRecord> out, long nowMs, String reason) {
        if (t.pendingDeliveredFire && (ev.newState() != AlertState.FIRING
                || t.deliveredFireOccurrence != ev.fireOccurrence())) {
            deliverRecovery(t, ev, null, out, nowMs,
                    "recovery conclusion after " + reason);
        }
        if (ev.newState() == AlertState.FIRING) {
            if (!t.pendingDeliveredFire && t.deliveredFireOccurrence < ev.fireOccurrence()) {
                if (cooldownActive(t, nowMs)) {
                    out.add(record(NotificationKind.FIRED,
                            NotificationDecision.SUPPRESSED_COOLDOWN, ev, null,
                            t.notificationRound,
                            "cooldown active; fire occurrence " + ev.fireOccurrence()
                                    + " still pending notification (" + reason + ")", nowMs));
                } else {
                    deliverFire(t, ev, null, out, nowMs,
                            "fire conclusion after " + reason);
                }
            } else if (t.pendingDeliveredFire && cooldownMs > 0
                    && t.deliveredFireOccurrence == ev.fireOccurrence()
                    && nowMs - t.lastFireNotifiedAtMs >= cooldownMs) {
                deliverFire(t, ev, null, out, nowMs,
                        "still firing after cooldown (" + reason + ")");
            }
        }
    }

    private void deliverFire(NotificationTracker t, AlertEvaluation ev, AlertEvent source,
                             List<NotificationRecord> out, long nowMs, String detail) {
        t.notificationRound++;
        t.lastFireNotifiedAtMs = nowMs;
        t.deliveredFireOccurrence = ev.fireOccurrence();
        t.pendingDeliveredFire = true;
        out.add(record(NotificationKind.FIRED, NotificationDecision.DELIVERED, ev, source,
                t.notificationRound, detail + "; round=" + t.notificationRound, nowMs));
    }

    private void deliverRecovery(NotificationTracker t, AlertEvaluation ev, AlertEvent source,
                                 List<NotificationRecord> out, long nowMs, String detail) {
        t.pendingDeliveredFire = false;
        out.add(record(NotificationKind.RECOVERED, NotificationDecision.DELIVERED, ev, source,
                t.notificationRound, detail + "; round=" + t.notificationRound, nowMs));
    }

    private boolean cooldownActive(NotificationTracker t, long nowMs) {
        return cooldownMs > 0 && t.lastFireNotifiedAtMs != Long.MIN_VALUE
                && nowMs - t.lastFireNotifiedAtMs < cooldownMs;
    }

    private NotificationTracker trackerFor(AlertEvaluation ev, long nowMs) {
        String key = ev.ruleId() + "|" + ev.fingerprint();
        NotificationTracker t = trackers.get(key);
        if (t != null) {
            return t;
        }
        if (trackers.size() >= maxTrackers) {
            evictTrackers(nowMs);
            if (trackers.size() >= maxTrackers) {
                return null;
            }
        }
        t = new NotificationTracker(ev.ruleId(), ev.fingerprint(), ev.labels());
        trackers.put(key, t);
        return t;
    }

    /** 回收：规则已删除的、或长时间无评估活动的跟踪器。 */
    private void evictTrackers(long nowMs) {
        Iterator<Map.Entry<String, NotificationTracker>> it = trackers.entrySet().iterator();
        while (it.hasNext()) {
            NotificationTracker t = it.next().getValue();
            boolean ruleGone = ruleRegistry.get(t.ruleId) == null;
            boolean idle = nowMs - t.updatedAtMs > trackerIdleMs;
            if (ruleGone || idle) {
                it.remove();
            }
        }
    }

    /** 规则删除时立即回收其全部跟踪器，返回回收数量。 */
    public synchronized int purgeRule(String ruleId) {
        int removed = 0;
        Iterator<Map.Entry<String, NotificationTracker>> it = trackers.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().ruleId.equals(ruleId)) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    private NotificationRecord record(NotificationKind kind, NotificationDecision decision,
                                      AlertEvaluation ev, AlertEvent source, int round,
                                      String detail, long nowMs) {
        recordSeq++;
        String id = "ntf:" + ev.ruleId() + ":" + ev.fingerprint() + ":" + round + ":"
                + kind.name().toLowerCase() + ":" + ev.windowStartMs() + ":" + recordSeq;
        return new NotificationRecord(id, kind, decision, ev.ruleId(), ev.fingerprint(),
                ev.labels(), ev.windowStartMs(), ev.observedValue(), ev.fireOccurrence(),
                round, source == null ? null : source.eventId(), detail, nowMs);
    }

    /** 单个维度的跟踪器快照（不存在时返回 null）。 */
    public synchronized TrackerSnapshot tracker(String ruleId, String fingerprint) {
        NotificationTracker t = trackers.get(ruleId + "|" + fingerprint);
        return t == null ? null : snapshot(t);
    }

    /** 全部跟踪器快照，按（规则, 维度）排序，顺序确定。 */
    public synchronized List<TrackerSnapshot> trackers() {
        List<TrackerSnapshot> out = new ArrayList<>();
        for (NotificationTracker t : trackers.values()) {
            out.add(snapshot(t));
        }
        out.sort(java.util.Comparator.comparing(TrackerSnapshot::ruleId)
                .thenComparing(TrackerSnapshot::fingerprint));
        return out;
    }

    public synchronized int trackerCount() {
        return trackers.size();
    }

    private static TrackerSnapshot snapshot(NotificationTracker t) {
        return new TrackerSnapshot(t.ruleId, t.fingerprint, t.flapping, t.notificationRound,
                t.deliveredFireOccurrence, t.pendingDeliveredFire, t.transitionTimes.size(),
                t.lastFireNotifiedAtMs, t.updatedAtMs);
    }
}
