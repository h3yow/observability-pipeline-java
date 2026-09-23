package com.github.h3yow.observabilitypipelinejava.alert;

import com.github.h3yow.observabilitypipelinejava.model.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.model.AlertState;
import com.github.h3yow.observabilitypipelinejava.model.AlertStateSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 告警维度状态与事件存储（线程安全）。
 * key = ruleId + "|" + fingerprint。
 * 状态快照与事件均按更新时间有序，恢复后再次触发以 fireOccurrence 区分。
 */
public class AlertStateStore {

    private final Map<String, AlertStateSnapshot> states = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<AlertEvent> events = new CopyOnWriteArrayList<>();

    public String keyOf(String ruleId, String fingerprint) {
        return ruleId + "|" + fingerprint;
    }

    public AlertStateSnapshot get(String key) {
        return states.get(key);
    }

    public void put(AlertStateSnapshot snapshot) {
        states.put(keyOf(snapshot.ruleId(), snapshot.fingerprint()), snapshot);
    }

    public List<AlertStateSnapshot> listByRule(String ruleId) {
        List<AlertStateSnapshot> out = new ArrayList<>();
        for (AlertStateSnapshot s : states.values()) {
            if (s.ruleId().equals(ruleId)) {
                out.add(s);
            }
        }
        out.sort(java.util.Comparator.comparing(AlertStateSnapshot::fingerprint));
        return out;
    }

    public List<AlertStateSnapshot> listAll() {
        List<AlertStateSnapshot> out = new ArrayList<>(states.values());
        out.sort(java.util.Comparator.comparing(AlertStateSnapshot::ruleId)
                .thenComparing(AlertStateSnapshot::fingerprint));
        return out;
    }

    public void appendEvent(AlertEvent event) {
        events.add(event);
    }

    public List<AlertEvent> events(String ruleId) {
        List<AlertEvent> out = new ArrayList<>();
        for (AlertEvent e : events) {
            if (e.ruleId().equals(ruleId)) {
                out.add(e);
            }
        }
        out.sort(java.util.Comparator.comparingLong(AlertEvent::windowStartMs)
                .thenComparing(AlertEvent::fingerprint)
                .thenComparing(AlertEvent::type));
        return out;
    }

    public List<AlertEvent> allEvents() {
        return new ArrayList<>(events);
    }

    /** 当前处于给定状态的维度数量。 */
    public long countInState(AlertState state) {
        long n = 0;
        for (AlertStateSnapshot s : states.values()) {
            if (s.state() == state) {
                n++;
            }
        }
        return n;
    }
}
