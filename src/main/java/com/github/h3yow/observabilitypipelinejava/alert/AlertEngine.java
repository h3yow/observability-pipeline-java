package com.github.h3yow.observabilitypipelinejava.alert;

import com.github.h3yow.observabilitypipelinejava.domain.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.domain.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.domain.AlertRule;
import com.github.h3yow.observabilitypipelinejava.domain.AlertState;
import com.github.h3yow.observabilitypipelinejava.domain.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.storage.AlertRuleState;
import com.github.h3yow.observabilitypipelinejava.storage.AlertStateStore;
import com.github.h3yow.observabilitypipelinejava.storage.EventJournal;
import com.github.h3yow.observabilitypipelinejava.storage.RuleStateKey;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
import com.github.h3yow.observabilitypipelinejava.window.SeriesKey;
import com.github.h3yow.observabilitypipelinejava.window.WindowStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 告警引擎：管理规则、在窗口封板后做状态机评估。
 *
 * <p>状态迁移语义（每个已评估窗口迁移一次，同一窗口重复评估幂等）：
 * <ul>
 *   <li>INACTIVE + 条件满足 -> PENDING（不产生事件，条件未持续满足时绝不误报）</li>
 *   <li>PENDING + 条件连续满足达到 consecutiveFires 个窗口 -> FIRING（产生 FIRING 事件）；
 *       期间任一窗口条件不满足 -> 回退 INACTIVE，计数清零</li>
 *   <li>FIRING + 条件仍满足 -> 保持 FIRING，不重复产生事件（按 ruleId+标签维度去重）</li>
 *   <li>FIRING + 条件不满足 -> RESOLVED（产生 RESOLVED 事件）；下次评估自动回到 INACTIVE</li>
 *   <li>RESOLVED 是一个可被查询到的瞬态；恢复后再次满足将产生全新序号的 FIRING 事件</li>
 * </ul>
 */
public class AlertEngine {

    private final WindowStore windowStore;
    private final AlertStateStore stateStore;
    private final EventJournal journal;
    private final Clock clock;
    private final Map<String, AlertRule> rules = new ConcurrentHashMap<>();

    public AlertEngine(WindowStore windowStore, AlertStateStore stateStore, EventJournal journal,
                       Clock clock) {
        this.windowStore = windowStore;
        this.stateStore = stateStore;
        this.journal = journal;
        this.clock = clock;
    }

    public void addRule(AlertRule rule) {
        if (rule == null || rule.ruleId() == null || rule.ruleId().isBlank()) {
            throw new IllegalArgumentException("rule and ruleId must not be empty");
        }
        if (rule.consecutiveFires() < 1) {
            throw new IllegalArgumentException("consecutiveFires must be >= 1");
        }
        rules.put(rule.ruleId(), rule);
    }

    public AlertRule getRule(String ruleId) {
        return rules.get(ruleId);
    }

    public Map<String, AlertRule> rules() {
        return Map.copyOf(rules);
    }

    /**
     * 对某指标在某个已封板窗口上的全部相关规则做一次评估。
     * 规则按 ruleId 排序、序列按维度排序，因此同一输入总产出同一事件顺序。
     */
    public List<AlertEvent> evaluateWindow(String metric, long windowStart, long windowSizeMillis) {
        List<AlertEvent> produced = new ArrayList<>();
        List<AlertRule> relevant = new ArrayList<>();
        for (AlertRule rule : rules.values()) {
            if (rule.metric().equals(metric)) {
                relevant.add(rule);
            }
        }
        relevant.sort(java.util.Comparator.comparing(AlertRule::ruleId));
        for (AlertRule rule : relevant) {
            List<WindowAggregate> aggs =
                    windowStore.aggregatesForWindow(metric, windowStart, windowSizeMillis);
            for (WindowAggregate agg : aggs) {
                if (matchesFilter(rule, agg.labels())) {
                    produced.addAll(evaluateOne(rule, agg));
                }
            }
        }
        return produced;
    }

    /** 单序列单规则评估（主要便于单测），返回新事件（可能为空）。幂等。 */
    public List<AlertEvent> evaluateOne(AlertRule rule, WindowAggregate agg) {
        SeriesKey series = SeriesKey.of(rule.metric(), agg.labels());
        RuleStateKey key = new RuleStateKey(rule.ruleId(), series);
        AlertRuleState st = stateStore.getOrCreate(key);
        double observed = AlertEvaluator.extract(agg, rule.aggregation());
        boolean matches = AlertEvaluator.compare(observed, rule.op(), rule.threshold());
        List<AlertEvent> events = new ArrayList<>();

        synchronized (st) {
            // 幂等：同一窗口重复评估，不重复迁移/发事件
            if (st.lastEvaluatedWindowStart() == agg.windowStart()) {
                return events;
            }
            AlertState prev = st.state();
            // RESOLVED 为瞬态：进入下一个窗口评估时先回到 INACTIVE
            if (prev == AlertState.RESOLVED) {
                prev = AlertState.INACTIVE;
                st.state(AlertState.INACTIVE);
                st.consecutiveMatches(0);
            }

            AlertState next = prev;
            switch (prev) {
                case INACTIVE -> {
                    if (matches) {
                        // 连续 1 个窗口即触发的规则，首窗口直接 FIRING
                        if (rule.consecutiveFires() <= 1) {
                            next = AlertState.FIRING;
                            st.consecutiveMatches(1);
                            events.add(publish(rule, agg, series, AlertEventType.FIRING, observed,
                                    "condition " + rule.aggregation() + describe(rule)
                                            + " met for 1 window"));
                        } else {
                            next = AlertState.PENDING;
                            st.consecutiveMatches(1);
                        }
                    }
                }
                case PENDING -> {
                    if (matches) {
                        int n = st.consecutiveMatches() + 1;
                        st.consecutiveMatches(n);
                        if (n >= rule.consecutiveFires()) {
                            next = AlertState.FIRING;
                            events.add(publish(rule, agg, series, AlertEventType.FIRING, observed,
                                    "condition " + rule.aggregation() + describe(rule) + " met for "
                                            + n + " consecutive window(s)"));
                        }
                    } else {
                        // 条件未持续满足：回落，绝不误报
                        next = AlertState.INACTIVE;
                        st.consecutiveMatches(0);
                    }
                }
                case FIRING -> {
                    if (matches) {
                        // 同维度重复触发被去重，不堆积事件
                        next = AlertState.FIRING;
                    } else {
                        next = AlertState.RESOLVED;
                        st.consecutiveMatches(0);
                        events.add(publish(rule, agg, series, AlertEventType.RESOLVED, observed,
                                "condition " + rule.aggregation() + describe(rule)
                                        + " no longer met"));
                    }
                }
                default -> {
                    next = AlertState.INACTIVE;
                }
            }
            st.state(next);
            st.lastEvaluatedWindowStart(agg.windowStart());
            st.lastObservedValue(observed);
        }
        return events;
    }

    private AlertEvent publish(AlertRule rule, WindowAggregate agg, SeriesKey series,
                               AlertEventType type, double observed, String reason) {
        AlertEvent raw = new AlertEvent(0L, rule.ruleId(), rule.metric(), series.labels(), type,
                agg.windowStart(), observed, rule.threshold(), reason, clock.nowMillis());
        return journal.append(raw);
    }

    /** 规则维度状态快照（ruleId -> (维度字符串 -> 状态名)），按 ID/维度排序。 */
    public Map<String, Map<String, String>> stateSnapshot() {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        for (var entry : stateStore.snapshot()) {
            out.computeIfAbsent(entry.getKey().ruleId(), r -> new LinkedHashMap<>())
                    .put(entry.getKey().series().dimensionString(), entry.getValue().state().name());
        }
        return out;
    }

    public List<AlertEvent> events(String ruleId) {
        return ruleId == null ? journal.all() : journal.findByRule(ruleId);
    }

    private boolean matchesFilter(AlertRule rule, Map<String, String> labels) {
        if (rule.labelFilter() == null || rule.labelFilter().isEmpty()) {
            return true;
        }
        for (Map.Entry<String, String> f : rule.labelFilter().entrySet()) {
            String actual = labels.get(f.getKey());
            if (!java.util.Objects.equals(actual, f.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static String describe(AlertRule rule) {
        return " " + rule.op().name() + " " + rule.threshold();
    }
}
