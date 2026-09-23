package com.github.h3yow.observabilitypipelinejava.web;

import com.github.h3yow.observabilitypipelinejava.config.PipelineProperties;
import com.github.h3yow.observabilitypipelinejava.domain.IngestResult;
import com.github.h3yow.observabilitypipelinejava.domain.MetricSample;
import com.github.h3yow.observabilitypipelinejava.domain.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.ingest.IngestService;
import com.github.h3yow.observabilitypipelinejava.service.MetricsQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 指标对外入口：单条/批量上报与聚合查询。
 * 每条样本都返回可区分的受理结论与原因，且服务端打印输入+判定日志便于人工核对。
 */
@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {

    private static final Logger log = LoggerFactory.getLogger(MetricsController.class);

    private final IngestService ingestService;
    private final MetricsQueryService queryService;
    private final PipelineProperties props;

    public MetricsController(IngestService ingestService, MetricsQueryService queryService,
                             PipelineProperties props) {
        this.ingestService = ingestService;
        this.queryService = queryService;
        this.props = props;
    }

    /** 单条上报。 */
    @PostMapping("/samples")
    public Map<String, Object> ingest(@RequestBody Map<String, Object> body) {
        MetricSample sample = toSample(body);
        IngestResult result = ingestService.ingest(sample);
        log.info("ingest single input={} => status={} reason={} window={} late={} outOfOrder={} detail={}",
                describe(sample), result.status(), result.reason(), result.windowStart(),
                result.late(), result.outOfOrder(), result.detail());
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("input", describe(sample));
        response.putAll(resultView(result));
        return response;
    }

    /** 批量上报：逐条给出可区分的受理结果。 */
    @PostMapping("/samples:batch")
    public Map<String, Object> batch(@RequestBody List<Map<String, Object>> body) {
        List<Map<String, Object>> items = new ArrayList<>();
        int accepted = 0;
        int rejected = 0;
        if (body != null) {
            for (Map<String, Object> item : body) {
                MetricSample sample = toSample(item);
                IngestResult result = ingestService.ingest(sample);
                if (result.status().name().startsWith("ACCEPTED")) {
                    accepted++;
                } else {
                    rejected++;
                }
                log.info("ingest batch  input={} => status={} reason={} window={} late={} outOfOrder={} detail={}",
                        describe(sample), result.status(), result.reason(), result.windowStart(),
                        result.late(), result.outOfOrder(), result.detail());
                Map<String, Object> view = new LinkedHashMap<>();
                view.put("input", describe(sample));
                view.putAll(resultView(result));
                items.add(view);
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("total", items.size());
        response.put("accepted", accepted);
        response.put("rejected", rejected);
        response.put("results", items);
        return response;
    }

    /** 聚合查询：GET /api/v1/metrics/{metric}/aggregates?from=&to= */
    @GetMapping("/{metric}/aggregates")
    public Map<String, Object> query(@PathVariable String metric,
                                     @RequestParam long from,
                                     @RequestParam long to) {
        List<WindowAggregate> aggs = queryService.query(metric, from, to, props.getWindowSizeMillis());
        List<Map<String, Object>> items = new ArrayList<>();
        for (WindowAggregate a : aggs) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("metric", a.metric());
            m.put("labels", new TreeMap<>(a.labels()));
            m.put("windowStart", a.windowStart());
            m.put("windowEnd", a.windowEnd());
            m.put("count", a.count());
            m.put("sum", a.sum());
            m.put("min", a.min());
            m.put("max", a.max());
            m.put("avg", a.avg());
            m.put("lastTimestamp", a.lastTimestamp());
            items.add(m);
        }
        log.info("query metric={} from={} to={} => {} aggregates", metric, from, to, items.size());
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metric", metric);
        response.put("windowSizeMillis", props.getWindowSizeMillis());
        response.put("aggregates", items);
        return response;
    }

    private Map<String, Object> resultView(IngestResult r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", r.status().name());
        m.put("reason", r.reason().name());
        m.put("windowStart", r.windowStart());
        m.put("late", r.late());
        m.put("outOfOrder", r.outOfOrder());
        m.put("detail", r.detail());
        return m;
    }

    private static String describe(MetricSample s) {
        if (s == null) {
            return "<null sample>";
        }
        return s.metric() + new TreeMap<>(s.labels() == null ? Map.of() : s.labels())
                + " value=" + s.value() + " ts=" + s.timestamp();
    }

    /** 请求体 -> MetricSample；数值字段缺失按 NaN 处理（会被合法性校验拒绝）。 */
    @SuppressWarnings("unchecked")
    static MetricSample toSample(Map<String, Object> body) {
        if (body == null) {
            return new MetricSample(null, Map.of(), Double.NaN, 0L);
        }
        String metric = body.get("metric") == null ? null : String.valueOf(body.get("metric"));
        Object v = body.get("value");
        double value = (v instanceof Number n) ? n.doubleValue() : Double.NaN;
        long ts = body.get("timestamp") instanceof Number t ? t.longValue() : 0L;
        Map<String, String> labels = new TreeMap<>();
        if (body.get("labels") instanceof Map<?, ?> raw) {
            for (Map.Entry<?, ?> e : raw.entrySet()) {
                labels.put(String.valueOf(e.getKey()),
                        e.getValue() == null ? "" : String.valueOf(e.getValue()));
            }
        }
        return new MetricSample(metric, labels, value, ts);
    }
}
