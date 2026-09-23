package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** HTTP 端到端冒烟：受理结果、规则、封存、压力接口全部可达且语义正确。 */
@SpringBootTest(properties = {
        "observability.journal-enabled=false",
        "observability.replay-on-startup=false",
        "observability.window-size-ms=10000",
        "observability.lateness-ms=30000",
        "observability.retention-ms=100000",
        "observability.max-series-per-metric=2",
        "observability.max-global-series=5",
        "observability.rate-limit-per-sec=100000"
})
@AutoConfigureMockMvc
class ObservabilityApiTest {

    private static final String INGEST = "/api/v1/metrics/ingest";

    @Autowired
    private MockMvc mvc;

    private String sample(String host, long ts, double value) {
        return """
                {"metric":"cpu","labels":{"host":"%s"},"timestampMs":%d,"value":%s,"sourceId":"it"}"""
                .formatted(host, ts, value);
    }

    private String sampleOpen(String host, double value) {
        return """
                {"metric":"cpu","labels":{"host":"%s"},"value":%s,"sourceId":"it"}"""
                .formatted(host, value);
    }

    @Test
    void ingestRuleTickPressureFlow() throws Exception {
        long now = System.currentTimeMillis();
        // 待封存窗口（约 15 秒前，处于 30 秒迟到宽限期内）：作为告警输入
        long w0 = ((now - 15_000) / 10_000) * 10_000;
        // 基数控制样本不提供时间戳，由服务端补当前时间，恒落在开放窗口

        // 1) 迟到但在宽限期内的样本被受理（不静默丢弃）
        mvc.perform(post(INGEST).contentType("application/json")
                        .content(sample("a", w0 + 1_000, 150.0)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.status").value("ACCEPTED_LATE"));

        // 2) 异常输入：未来时间戳拒绝，原因可区分
        mvc.perform(post(INGEST).contentType("application/json")
                        .content("""
                                {"metric":"cpu","labels":{},"timestampMs":99999999999999,
                                 "value":1.0,"sourceId":"it"}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("TOO_FAR_FUTURE"));

        // 3) 基数上限 2：当前窗口两个序列放行，第三个被拒
        mvc.perform(post(INGEST).contentType("application/json")
                        .content(sampleOpen("a", 1.0)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));
        mvc.perform(post(INGEST).contentType("application/json")
                        .content(sampleOpen("b", 1.0)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));
        mvc.perform(post(INGEST).contentType("application/json")
                        .content(sampleOpen("c", 1.0)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CARDINALITY_LIMIT"));

        // 4) 规则创建
        mvc.perform(put("/api/v1/rules").contentType("application/json")
                        .content("""
                                {"id":"r-cpu","metric":"cpu","labelSelector":{},"function":"AVG",
                                 "comparator":"GT","threshold":100.0,"fireAfterWindows":1}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("r-cpu"));

        // 5) 等待 w0 越过封存线（end + lateness），消除毫秒级时序抖动
        long sealLine = w0 + 10_000 + 30_000;
        long sleep = sealLine - System.currentTimeMillis() + 50;
        if (sleep > 0) {
            Thread.sleep(sleep);
        }
        String tickBody = mvc.perform(post("/api/v1/admin/tick"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertTrue(tickBody.contains("\"sealedWindows\":1"), tickBody);

        // 6) 告警状态：host=a 在 w0 的 AVG=150 > 100，fireAfterWindows=1 -> FIRING
        String alerts = mvc.perform(get("/api/v1/alerts").param("ruleId", "r-cpu"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertNotNull(alerts);
        assertTrue(alerts.contains("FIRING"), alerts);

        // 7) 压力自观测：基数超限可观测、原因可区分
        mvc.perform(get("/api/v1/pressure"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.metricCardinalityLimited").value(true));

        // 8) 聚合查询
        mvc.perform(get("/api/v1/metrics")
                        .param("metric", "cpu")
                        .param("windowStartMs", String.valueOf(w0)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].metric").value("cpu"));

        // 9) 事件可查询
        mvc.perform(get("/api/v1/alerts/events").param("ruleId", "r-cpu"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].type").value("FIRED"));
    }
}
