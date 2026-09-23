package com.github.h3yow.observabilitypipelinejava.web;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.github.h3yow.observabilitypipelinejava.alert.AlertEngine;
import com.github.h3yow.observabilitypipelinejava.domain.Aggregation;
import com.github.h3yow.observabilitypipelinejava.domain.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.domain.AlertRule;
import com.github.h3yow.observabilitypipelinejava.domain.ComparisonOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

/**
 * 告警规则管理、状态与事件查询入口。
 */
@RestController
@RequestMapping("/api/v1/alerts")
public class AlertController {

    private static final Logger log = LoggerFactory.getLogger(AlertController.class);

    private final AlertEngine alertEngine;

    public AlertController(AlertEngine alertEngine) {
        this.alertEngine = alertEngine;
    }

    /** 创建规则。 */
    @PostMapping("/rules")
    public Map<String, Object> createRule(@RequestBody Map<String, Object> body) {
        AlertRule rule = toRule(body);
        alertEngine.addRule(rule);
        log.info("alert rule added: id={} metric={} agg={} op={} threshold={} consecutiveFires={} filter={}",
                rule.ruleId(), rule.metric(), rule.aggregation(), rule.op(), rule.threshold(),
                rule.consecutiveFires(), rule.labelFilter());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ruleId", rule.ruleId());
        m.put("metric", rule.metric());
        m.put("aggregation", rule.aggregation().name());
        m.put("op", rule.op().name());
        m.put("threshold", rule.threshold());
        m.put("consecutiveFires", rule.consecutiveFires());
        m.put("labelFilter", rule.labelFilter());
        return m;
    }

    @GetMapping("/rules")
    public List<Map<String, Object>> listRules() {
        List<Map<String, Object>> list = new ArrayList<>();
        alertEngine.rules().keySet().stream().sorted().forEach(id -> {
            AlertRule r = alertEngine.getRule(id);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ruleId", r.ruleId());
            m.put("metric", r.metric());
            m.put("aggregation", r.aggregation().name());
            m.put("op", r.op().name());
            m.put("threshold", r.threshold());
            m.put("consecutiveFires", r.consecutiveFires());
            m.put("labelFilter", r.labelFilter());
            list.add(m);
        });
        return list;
    }

    /** 规则状态：ruleId -> (维度字符串 -> 状态名)。 */
    @GetMapping("/states")
    public Map<String, Map<String, String>> states() {
        return alertEngine.stateSnapshot();
    }

    @GetMapping("/events")
    public List<AlertEvent> events(@RequestParam(required = false) String ruleId) {
        return alertEngine.events(ruleId);
    }

    /** 请求体 -> AlertRule（默认 AVG/GT，连续窗口数 1）。 */
    @SuppressWarnings("unchecked")
    static AlertRule toRule(Map<String, Object> body) {
        String ruleId = String.valueOf(body.get("ruleId"));
        String metric = String.valueOf(body.get("metric"));
        Aggregation agg = body.get("aggregation") == null
                ? Aggregation.AVG
                : Aggregation.valueOf(String.valueOf(body.get("aggregation")).toUpperCase());
        ComparisonOp op = body.get("op") == null
                ? ComparisonOp.GT
                : ComparisonOp.valueOf(String.valueOf(body.get("op")).toUpperCase());
        double threshold = body.get("threshold") instanceof Number n ? n.doubleValue() : 0d;
        int fires = body.get("consecutiveFires") instanceof Number n ? n.intValue() : 1;
        Map<String, String> filter = new TreeMap<>();
        if (body.get("labelFilter") instanceof Map<?, ?> raw) {
            for (Map.Entry<?, ?> e : raw.entrySet()) {
                filter.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
            }
        }
        return new AlertRule(ruleId, metric, agg, op, threshold, fires, filter);
    }
}
