package com.github.h3yow.observabilitypipelinejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 端到端冒烟：异常输入有结构化拒绝结论，合法链路可查询统计。
 */
@SpringBootTest(properties = {
        "observability.window-size-millis=60000",
        "observability.grace-period-millis=10000",
        "observability.max-series-per-metric=50",
        "observability.max-series-global=200"
})
@AutoConfigureMockMvc
public class WebApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(WebApiIntegrationTest.class);

    @Autowired
    private MockMvc mvc;

    @Test
    void invalidSampleReturnsStructuredRejection() throws Exception {
        String body = "{\"metric\":\"cpu\",\"labels\":{\"\":\"x\"},\"value\":1,\"timestamp\":0}";
        log.info("POST /samples invalid body={}", body);
        mvc.perform(post("/api/v1/metrics/samples")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.reason").value("EMPTY_LABEL_KEY"));
    }

    @Test
    void ingestBatchRuleStatsFlow() throws Exception {
        String m1 = "{\"metric\":\"webm\",\"labels\":{\"host\":\"a\"},\"value\":95,\"timestamp\":%d}";
        String m2 = "{\"metric\":\"webm\",\"labels\":{\"host\":\"a\"},\"value\":5,\"timestamp\":%d}";
        long now = System.currentTimeMillis();
        String batch = "[" + String.format(m1, now - 2_000) + "," + String.format(m1, now - 1_000)
                + "," + String.format(m2, now - 500) + "]";
        log.info("POST batch {}", batch);
        mvc.perform(post("/api/v1/metrics/samples:batch")
                        .contentType(MediaType.APPLICATION_JSON).content(batch))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(3))
                .andExpect(jsonPath("$.accepted").value(3));

        mvc.perform(get("/api/v1/admin/stats"))
                .andDo(r -> log.info("GET /admin/stats => {}", r.getResponse().getContentAsString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(3));

        mvc.perform(get("/api/v1/metrics/webm/aggregates")
                        .param("from", String.valueOf(now - 60_000))
                        .param("to", String.valueOf(now + 60_000)))
                .andDo(r -> log.info("GET aggregates => {}", r.getResponse().getContentAsString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.aggregates[0].count").value(3));
    }
}
