package com.github.h3yow.observabilitypipelinejava.storage;

import com.github.h3yow.observabilitypipelinejava.domain.AlertState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 规则状态存储：内存承载。状态条目自身的读写在条目锁内完成，
 * 保证同一（规则, 维度）的状态迁移是原子且确定的。
 */
public class AlertStateStore {

    private final ConcurrentHashMap<RuleStateKey, AlertRuleState> states = new ConcurrentHashMap<>();

    /** 获取或创建某规则维度的状态（初始 INACTIVE）。 */
    public AlertRuleState getOrCreate(RuleStateKey key) {
        return states.computeIfAbsent(key, k -> new AlertRuleState(AlertState.INACTIVE));
    }

    /** 某规则的全部维度键快照（按维度字符串排序，保证遍历顺序确定）。 */
    public Set<RuleStateKey> keysForRule(String ruleId) {
        List<RuleStateKey> keys = new ArrayList<>();
        for (RuleStateKey key : states.keySet()) {
            if (key.ruleId().equals(ruleId)) {
                keys.add(key);
            }
        }
        keys.sort(java.util.Comparator.comparing(k -> k.series().dimensionString()));
        return new java.util.LinkedHashSet<>(keys);
    }

    /** 全部状态条目快照。 */
    public List<Map.Entry<RuleStateKey, AlertRuleState>> snapshot() {
        List<Map.Entry<RuleStateKey, AlertRuleState>> list = new ArrayList<>(states.entrySet());
        list.sort(java.util.Comparator
                .comparing((Map.Entry<RuleStateKey, AlertRuleState> e) -> e.getKey().ruleId())
                .thenComparing(e -> e.getKey().series().dimensionString()));
        return list;
    }

    /** 测试辅助：清空。 */
    public void clear() {
        states.clear();
    }
}
