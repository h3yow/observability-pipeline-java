package com.github.h3yow.observabilitypipelinejava.alert;

import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** 规则注册中心（本地内存，线程安全）。 */
public class AlertRuleRegistry {

    private final ConcurrentHashMap<String, AlertRule> rules = new ConcurrentHashMap<>();

    /** 新增或替换规则。 */
    public AlertRule put(AlertRule rule) {
        if (rule == null || rule.id() == null || rule.id().isBlank()) {
            throw new IllegalArgumentException("rule id is required");
        }
        if (rule.fireAfterWindows() < 1) {
            throw new IllegalArgumentException("fireAfterWindows must be >= 1");
        }
        rules.put(rule.id(), rule);
        return rule;
    }

    public AlertRule get(String id) {
        return rules.get(id);
    }

    public List<AlertRule> list() {
        List<AlertRule> out = new ArrayList<>(rules.values());
        out.sort(java.util.Comparator.comparing(AlertRule::id));
        return out;
    }

    public boolean remove(String id) {
        return rules.remove(id) != null;
    }
}
