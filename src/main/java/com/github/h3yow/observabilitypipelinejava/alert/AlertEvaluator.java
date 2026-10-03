package com.github.h3yow.observabilitypipelinejava.alert;

import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.model.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.AlertState;
import com.github.h3yow.observabilitypipelinejava.model.AlertStateSnapshot;
import com.github.h3yow.observabilitypipelinejava.model.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.notify.NotificationController;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 告警判定器：对每个已封存窗口推进状态机。
 *
 * <pre>
 *   INACTIVE ── 条件连续命中 1 窗口 ──▶ PENDING
 *      ▲  │                              │
 *      │  └── 任一窗口不满足（清零）        │ 条件继续命中，累计到 fireAfterWindows
 *      │                                 ▼
 *      │                            FIRING（产生 FIRED 事件）
 *      │                                 │ 条件不满足
 *      │                                 ▼
 *      └─────────────────────────── RECOVERED（产生 RECOVERED 事件）
 * </pre>
 * 规则：
 * <ul>
 *   <li>未持续满足（PENDING 中断）立即回到 INACTIVE 且不产生 FIRED，杜绝误报；</li>
 *   <li>FIRING 期间重复命中不产生重复事件（按 ruleId+fingerprint 维度去重）；</li>
 *   <li>恢复后再次触发：fireOccurrence 递增，eventId 不同，事件可区分；</li>
 *   <li>每个 (规则, 维度) 对每个窗口只评估一次（窗口起点必须推进），结论可复现。</li>
 * </ul>
 */
public class AlertEvaluator {

    private final AlertRuleRegistry ruleRegistry;
    private final AlertStateStore stateStore;
    private final NotificationController notificationController;
    /** 通知跟踪状态的老化阈值（与窗口保留期对齐）。 */
    private final long trackerMaxAgeMs;
    private long eventSeq;

    public AlertEvaluator(AlertRuleRegistry ruleRegistry, AlertStateStore stateStore,
                          NotificationController notificationController,
                          long trackerMaxAgeMs) {
        this.ruleRegistry = ruleRegistry;
        this.stateStore = stateStore;
        this.notificationController = notificationController;
        this.trackerMaxAgeMs = trackerMaxAgeMs;
    }

    /**
     * 按窗口起点升序评估一批已封存窗口，返回本次产生的事件
     * （按窗口、规则、维度排序，顺序确定）。
     */
    public synchronized List<AlertEvent> evaluate(List<WindowAggregate> sealedWindows,
                                                  long evaluatedAtMs) {
        List<AlertEvent> generated = new ArrayList<>();
        List<WindowAggregate> ordered = new ArrayList<>(sealedWindows);
        ordered.sort(Comparator.comparingLong(WindowAggregate::getWindowStartMs)
                .thenComparing(WindowAggregate::getSeriesKey));

        for (WindowAggregate agg : ordered) {
            for (AlertRule rule : ruleRegistry.list()) {
                if (!rule.metric().equals(agg.getMetric())) {
                    continue;
                }
                if (!matchesSelector(agg.getLabels(), rule.labelSelector())) {
                    continue;
                }
                evaluateOne(rule, agg, generated, evaluatedAtMs);
            }
        }
        generated.sort(Comparator.comparingLong(AlertEvent::windowStartMs)
                .thenComparing(AlertEvent::ruleId)
                .thenComparing(AlertEvent::fingerprint)
                .thenComparing(AlertEvent::type));
        for (AlertEvent e : generated) {
            stateStore.appendEvent(e);
        }
        // 批次结束：回收过期静默与老化的冷却/抖动跟踪条目。
        notificationController.prune(evaluatedAtMs, trackerMaxAgeMs);
        return generated;
    }

    private void evaluateOne(AlertRule rule, WindowAggregate agg, List<AlertEvent> generated,
                             long evaluatedAtMs) {
        String key = stateStore.keyOf(rule.id(), agg.getFingerprint());
        AlertStateSnapshot prev = stateStore.get(key);
        double observed = agg.valueOf(rule.function());
        boolean matched = rule.comparator().matches(observed, rule.threshold());
        long windowStart = agg.getWindowStartMs();

        AlertState state = prev == null ? AlertState.INACTIVE : prev.state();
        int consecutive = prev == null ? 0 : prev.consecutiveMatchedWindows();
        int occurrence = prev == null ? 0 : prev.fireOccurrence();

        // 同一窗口不允许重复评估：窗口起点必须推进，保证结论可复现。
        long prevWindow = prev == null ? Long.MIN_VALUE : prev.lastWindowStartMs();
        if (windowStart <= prevWindow) {
            return;
        }

        // 本维度本窗口产生的生命周期事件（先收集，统一交给通知控制层判定）。
        List<AlertEvent> dimensionEvents = new ArrayList<>();

        switch (state) {
            case INACTIVE -> {
                if (matched) {
                    consecutive = 1;
                    if (consecutive >= rule.fireAfterWindows()) {
                        state = AlertState.FIRING;
                        occurrence += 1;
                        dimensionEvents.add(event(AlertEventType.FIRED, rule, agg, observed,
                                occurrence, evaluatedAtMs));
                    } else {
                        state = AlertState.PENDING;
                        dimensionEvents.add(event(AlertEventType.PENDING_STARTED, rule, agg, observed,
                                occurrence, evaluatedAtMs));
                    }
                } else {
                    consecutive = 0;
                    state = AlertState.INACTIVE;
                }
            }
            case RECOVERED -> {
                // 恢复后再观察一个判定周期：不命中则静默回到 INACTIVE（不再产生 RECOVERED）；
                // 命中则作为新一轮周期，从 1 开始累计。
                if (matched) {
                    consecutive = 1;
                    if (consecutive >= rule.fireAfterWindows()) {
                        state = AlertState.FIRING;
                        occurrence += 1;
                        dimensionEvents.add(event(AlertEventType.FIRED, rule, agg, observed,
                                occurrence, evaluatedAtMs));
                    } else {
                        state = AlertState.PENDING;
                        dimensionEvents.add(event(AlertEventType.PENDING_STARTED, rule, agg, observed,
                                occurrence, evaluatedAtMs));
                    }
                } else {
                    consecutive = 0;
                    state = AlertState.INACTIVE;
                }
            }
            case PENDING -> {
                if (matched) {
                    consecutive += 1;
                    if (consecutive >= rule.fireAfterWindows()) {
                        state = AlertState.FIRING;
                        occurrence += 1;
                        dimensionEvents.add(event(AlertEventType.FIRED, rule, agg, observed,
                                occurrence, evaluatedAtMs));
                    }
                } else {
                    // 条件未持续满足：清零回 INACTIVE，绝不错误触发。
                    consecutive = 0;
                    state = AlertState.INACTIVE;
                }
            }
            case FIRING -> {
                if (matched) {
                    // 维度去重：FIRING 期间不产生新的 FIRED 事件，仅延续观察值。
                    consecutive = Math.min(consecutive + 1, Integer.MAX_VALUE - 1);
                    state = AlertState.FIRING;
                } else {
                    consecutive = 0;
                    state = AlertState.RECOVERED;
                    dimensionEvents.add(event(AlertEventType.RECOVERED, rule, agg, observed,
                            occurrence, evaluatedAtMs));
                }
            }
        }

        AlertStateSnapshot current = new AlertStateSnapshot(rule.id(), agg.getFingerprint(),
                agg.getLabels(), state, consecutive, observed, windowStart, occurrence,
                evaluatedAtMs);
        stateStore.put(current);
        // 通知控制层：状态结论已落定，仅决定对外通知（静默/冷却/抖动）。
        notificationController.onDimensionEvaluated(rule, agg, current, matched,
                dimensionEvents, evaluatedAtMs);
        generated.addAll(dimensionEvents);
    }

    private boolean matchesSelector(Map<String, String> labels, Map<String, String> selector) {
        if (selector == null || selector.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, String> e : selector.entrySet()) {
            String want = e.getValue() == null ? "" : e.getValue();
            String got = labels.get(e.getKey());
            if (!want.equals(got)) {
                return false;
            }
        }
        return true;
    }

    private AlertEvent event(AlertEventType type, AlertRule rule, WindowAggregate agg,
                             double observed, int occurrence, long evaluatedAtMs) {
        String eventId = rule.id() + ":" + agg.getFingerprint() + ":" + occurrence
                + ":" + type.name().toLowerCase() + ":" + agg.getWindowStartMs();
        return new AlertEvent(eventId, type, rule.id(), agg.getFingerprint(), agg.getLabels(),
                agg.getWindowStartMs(), agg.getWindowSizeMs(), observed,
                rule.function(), rule.comparator(), rule.threshold(), occurrence,
                evaluatedAtMs);
    }
}
