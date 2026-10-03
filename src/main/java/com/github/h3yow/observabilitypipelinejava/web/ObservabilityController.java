package com.github.h3yow.observabilitypipelinejava.web;

import com.github.h3yow.observabilitypipelinejava.alert.AlertRuleRegistry;
import com.github.h3yow.observabilitypipelinejava.alert.AlertStateStore;
import com.github.h3yow.observabilitypipelinejava.model.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.aggregate.WindowAggregator;
import com.github.h3yow.observabilitypipelinejava.engine.PipelineEngine;
import com.github.h3yow.observabilitypipelinejava.ingest.IngestService;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.IngestResult;
import com.github.h3yow.observabilitypipelinejava.model.MetricSample;
import com.github.h3yow.observabilitypipelinejava.model.SilenceCreateStatus;
import com.github.h3yow.observabilitypipelinejava.model.WindowState;
import com.github.h3yow.observabilitypipelinejava.notify.NotificationController;
import com.github.h3yow.observabilitypipelinejava.notify.SilenceStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 本地可观测性管线对外入口。 */
@RestController
@RequestMapping("/api/v1")
public class ObservabilityController {

    private final IngestService ingestService;
    private final WindowAggregator aggregator;
    private final AlertRuleRegistry ruleRegistry;
    private final AlertStateStore alertStateStore;
    private final NotificationController notificationController;
    private final PipelineEngine engine;

    public ObservabilityController(IngestService ingestService,
                                   WindowAggregator aggregator,
                                   AlertRuleRegistry ruleRegistry,
                                   AlertStateStore alertStateStore,
                                   NotificationController notificationController,
                                   PipelineEngine engine) {
        this.ingestService = ingestService;
        this.aggregator = aggregator;
        this.ruleRegistry = ruleRegistry;
        this.alertStateStore = alertStateStore;
        this.notificationController = notificationController;
        this.engine = engine;
    }

    @PostMapping("/metrics/ingest")
    public IngestResult ingest(@RequestBody MetricSampleRequest request) {
        if (request == null || request.metric() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "request body and 'metric' are required");
        }
        return ingestService.ingest(toSample(request));
    }

    @PostMapping("/metrics/ingest-batch")
    public List<IngestResult> ingestBatch(@RequestBody BatchIngestRequest request) {
        if (request == null || request.samples() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "'samples' is required");
        }
        List<MetricSample> samples = new ArrayList<>();
        for (MetricSampleRequest r : request.samples()) {
            samples.add(toSample(r));
        }
        return ingestService.ingestBatch(samples);
    }

    @GetMapping("/metrics")
    public Object query(@RequestParam String metric,
                        @RequestParam long windowStartMs,
                        @RequestParam(required = false) String fingerprint) {
        if (fingerprint != null) {
            WindowAggregate agg = aggregator.query(metric, fingerprint, windowStartMs);
            if (agg == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "window not found");
            }
            return describe(agg);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (WindowAggregate agg : aggregator.queryByMetric(metric, windowStartMs)) {
            out.add(describe(agg));
        }
        return out;
    }

    @PutMapping("/rules")
    public AlertRule putRule(@RequestBody RuleRequest request) {
        try {
            return ruleRegistry.put(toRule(request));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    @GetMapping("/rules")
    public List<AlertRule> listRules() {
        return ruleRegistry.list();
    }

    @DeleteMapping("/rules/{id}")
    public Map<String, Object> deleteRule(@PathVariable String id) {
        boolean removed = ruleRegistry.remove(id);
        // 规则删除即刻回收其通知控制状态（静默/冷却/抖动跟踪）。
        notificationController.onRuleDeleted(id);
        return Map.of("id", id, "removed", removed);
    }

    @PutMapping("/silences")
    public Object putSilence(@RequestBody SilenceRequest request) {
        if (request == null || request.ruleId() == null || request.ruleId().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ruleId is required");
        }
        if (request.startMs() == null || request.endMs() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "startMs and endMs are required");
        }
        SilenceStore.CreateResult result = notificationController.silenceStore().create(
                request.ruleId(), request.labelSelector(), request.startMs(), request.endMs(),
                request.reason(), System.currentTimeMillis());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", result.status().name());
        if (result.silence() != null) {
            body.put("silence", result.silence());
        }
        // 各种异常情形以 HTTP 状态 + status 字段双重区分，不会默默生效或失效。
        HttpStatus http = switch (result.status()) {
            case CREATED -> HttpStatus.OK;
            case INVALID_RANGE, IN_PAST -> HttpStatus.BAD_REQUEST;
            case DUPLICATE, OVERLAP -> HttpStatus.CONFLICT;
            case LIMIT_REACHED -> HttpStatus.TOO_MANY_REQUESTS;
        };
        return org.springframework.http.ResponseEntity.status(http).body(body);
    }

    @GetMapping("/silences")
    public Object listSilences(@RequestParam(required = false) String ruleId) {
        return notificationController.silenceStore().list(ruleId);
    }

    @DeleteMapping("/silences/{id}")
    public Map<String, Object> cancelSilence(@PathVariable String id) {
        boolean cancelled = notificationController.silenceStore().cancel(id);
        return Map.of("id", id, "status", cancelled ? "CANCELLED" : "NOT_FOUND");
    }

    @GetMapping("/notifications")
    public Object listNotifications(@RequestParam(required = false) String ruleId) {
        return ruleId == null ? notificationController.allRecords()
                : notificationController.records(ruleId);
    }

    @GetMapping("/notifications/flapping")
    public Object flapping() {
        return notificationController.flapping();
    }

    @GetMapping("/notifications/status")
    public Object notificationStatus() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("silences", notificationController.silenceStore().size());
        body.put("flapTrackers", notificationController.flapTracker().size());
        body.put("flapTrackersEvicted", notificationController.flapTracker().evictedCount());
        body.put("cooldownTrackers", notificationController.cooldownSize());
        body.put("cooldownTrackersEvicted", notificationController.cooldownEvictedCount());
        body.put("notificationRecords", notificationController.allRecords().size());
        return body;
    }

    @GetMapping("/alerts")
    public Object listAlerts(@RequestParam(required = false) String ruleId) {
        return ruleId == null ? alertStateStore.listAll() : alertStateStore.listByRule(ruleId);
    }

    @GetMapping("/alerts/events")
    public Object listEvents(@RequestParam(required = false) String ruleId) {
        return ruleId == null ? alertStateStore.allEvents() : alertStateStore.events(ruleId);
    }

    @GetMapping("/pressure")
    public Object pressure() {
        return ingestService.pressure();
    }

    @PostMapping("/admin/tick")
    public Map<String, Object> tick() {
        PipelineEngine.TickResult result = engine.tick();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sealedWindows", result.sealedWindows());
        body.put("alertEvents", result.alertEvents());
        body.put("evictedWindows", result.evictedWindows());
        body.put("deregisteredSeries", result.deregisteredSeries());
        body.put("atMs", result.atMs());
        return body;
    }

    private static MetricSample toSample(MetricSampleRequest r) {
        return new MetricSample(r.metric(), r.labels(), r.timestampMs(),
                r.value() == null ? Double.NaN : r.value(), r.sourceId());
    }

    private static AlertRule toRule(RuleRequest r) {
        if (r == null) {
            throw new IllegalArgumentException("request body is required");
        }
        if (r.id() == null || r.id().isBlank()) {
            throw new IllegalArgumentException("rule id is required");
        }
        if (r.metric() == null || r.metric().isBlank()) {
            throw new IllegalArgumentException("metric is required");
        }
        AggregateFunction fn = r.function() == null ? AggregateFunction.AVG : r.function();
        ComparatorHolder.check(r);
        return new AlertRule(r.id(), r.metric(),
                r.labelSelector() == null ? Map.of() : r.labelSelector(),
                fn, r.comparator(), r.threshold(),
                r.fireAfterWindows() == null ? 1 : r.fireAfterWindows());
    }

    private static final class ComparatorHolder {
        static void check(RuleRequest r) {
            if (r.comparator() == null) {
                throw new IllegalArgumentException("comparator is required");
            }
            if (r.threshold() == null) {
                throw new IllegalArgumentException("threshold is required");
            }
            if (r.fireAfterWindows() != null && r.fireAfterWindows() < 1) {
                throw new IllegalArgumentException("fireAfterWindows must be >= 1");
            }
        }
    }

    private Map<String, Object> describe(WindowAggregate agg) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("metric", agg.getMetric());
        m.put("fingerprint", agg.getFingerprint());
        m.put("labels", agg.getLabels());
        m.put("seriesKey", agg.getSeriesKey());
        m.put("windowStartMs", agg.getWindowStartMs());
        m.put("windowSizeMs", agg.getWindowSizeMs());
        m.put("state", agg.getState() == WindowState.SEALED ? "SEALED" : "OPEN");
        m.put("count", agg.count());
        m.put("sum", agg.valueOf(AggregateFunction.SUM));
        m.put("avg", agg.valueOf(AggregateFunction.AVG));
        m.put("min", agg.valueOf(AggregateFunction.MIN));
        m.put("max", agg.valueOf(AggregateFunction.MAX));
        m.put("last", agg.valueOf(AggregateFunction.LAST));
        return m;
    }
}
