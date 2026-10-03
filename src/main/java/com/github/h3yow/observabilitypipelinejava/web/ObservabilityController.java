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
import com.github.h3yow.observabilitypipelinejava.model.SilenceOutcome;
import com.github.h3yow.observabilitypipelinejava.model.SilenceWindow;
import com.github.h3yow.observabilitypipelinejava.model.WindowState;
import com.github.h3yow.observabilitypipelinejava.notify.NotificationGate;
import com.github.h3yow.observabilitypipelinejava.notify.NotificationStore;
import com.github.h3yow.observabilitypipelinejava.notify.SilenceRegistry;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
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
    private final PipelineEngine engine;
    private final SilenceRegistry silenceRegistry;
    private final NotificationGate notificationGate;
    private final NotificationStore notificationStore;
    private final Clock clock;

    public ObservabilityController(IngestService ingestService,
                                   WindowAggregator aggregator,
                                   AlertRuleRegistry ruleRegistry,
                                   AlertStateStore alertStateStore,
                                   PipelineEngine engine,
                                   SilenceRegistry silenceRegistry,
                                   NotificationGate notificationGate,
                                   NotificationStore notificationStore,
                                   Clock clock) {
        this.ingestService = ingestService;
        this.aggregator = aggregator;
        this.ruleRegistry = ruleRegistry;
        this.alertStateStore = alertStateStore;
        this.engine = engine;
        this.silenceRegistry = silenceRegistry;
        this.notificationGate = notificationGate;
        this.notificationStore = notificationStore;
        this.clock = clock;
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
        // 规则删除后同步回收通知控制层状态，不留孤儿。
        int purgedTrackers = notificationGate.purgeRule(id);
        int purgedSilences = silenceRegistry.purgeRule(id);
        return Map.of("id", id, "removed", removed,
                "purgedTrackers", purgedTrackers, "purgedSilences", purgedSilences);
    }

    @PutMapping("/silences")
    public Map<String, Object> putSilence(@RequestBody SilenceRequest request) {
        if (request == null || request.ruleId() == null || request.ruleId().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ruleId is required");
        }
        if (request.startMs() == null || request.endMs() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "startMs and endMs are required");
        }
        long now = clock.nowMillis();
        java.util.Set<String> scope = request.fingerprints() == null
                ? java.util.Set.of() : new java.util.TreeSet<>(request.fingerprints());
        String id = request.id() == null || request.id().isBlank()
                ? SilenceWindow.deriveId(request.ruleId(), scope, request.startMs(),
                        request.endMs())
                : request.id();
        SilenceWindow window = new SilenceWindow(id, request.ruleId(), scope,
                request.startMs(), request.endMs(),
                request.reason() == null ? "" : request.reason(), now);
        SilenceOutcome outcome = silenceRegistry.create(window, now);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", outcome.code().name());
        body.put("ok", outcome.ok());
        body.put("detail", outcome.detail());
        if (outcome.silence() != null) {
            body.put("silence", describe(outcome.silence(), now));
        }
        return body;
    }

    @GetMapping("/silences")
    public List<Map<String, Object>> listSilences(@RequestParam(required = false) String ruleId) {
        long now = clock.nowMillis();
        List<Map<String, Object>> out = new ArrayList<>();
        for (SilenceWindow w : silenceRegistry.list(ruleId)) {
            out.add(describe(w, now));
        }
        return out;
    }

    @DeleteMapping("/silences/{id}")
    public Map<String, Object> cancelSilence(@PathVariable String id) {
        SilenceOutcome outcome = silenceRegistry.cancel(id);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", outcome.code().name());
        body.put("ok", outcome.ok());
        body.put("detail", outcome.detail());
        return body;
    }

    @GetMapping("/notifications")
    public Object listNotifications(@RequestParam(required = false) String ruleId) {
        return ruleId == null ? notificationStore.all() : notificationStore.list(ruleId);
    }

    @GetMapping("/notifications/trackers")
    public Object listTrackers() {
        return notificationGate.trackers();
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

    private Map<String, Object> describe(SilenceWindow w, long now) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", w.id());
        m.put("ruleId", w.ruleId());
        m.put("fingerprints", w.scopeList());
        m.put("startMs", w.startMs());
        m.put("endMs", w.endMs());
        m.put("reason", w.reason());
        m.put("createdAtMs", w.createdAtMs());
        m.put("status", w.statusAt(now).name());
        return m;
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
