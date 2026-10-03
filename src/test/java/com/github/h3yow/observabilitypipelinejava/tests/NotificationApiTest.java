package com.github.h3yow.observabilitypipelinejava.tests;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/** 通知控制 HTTP 冒烟：静默设置/查询/取消的结果可区分，通知台账与状态接口可达。 */
@SpringBootTest(properties = {
        "observability.journal-enabled=false",
        "observability.replay-on-startup=false"
})
@AutoConfigureMockMvc
class NotificationApiTest {

    @Autowired
    private MockMvc mvc;

    private String silenceBody(String ruleId, long startMs, long endMs) {
        return """
                {"ruleId":"%s","labelSelector":{},"startMs":%d,"endMs":%d,"reason":"it"}"""
                .formatted(ruleId, startMs, endMs);
    }

    @Test
    void silenceCrudAndNotificationStatusFlow() throws Exception {
        long now = System.currentTimeMillis();
        long start = now + 60_000;
        long end = now + 120_000;

        // 1) 创建成功
        MvcResult created = mvc.perform(put("/api/v1/silences").contentType("application/json")
                        .content(silenceBody("r-api", start, end)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.silence.ruleId").value("r-api"))
                .andReturn();
        String body = created.getResponse().getContentAsString();
        String id = body.replaceAll(".*\"id\":\"([^\"]+)\".*", "$1");

        // 2) 完全重复 → 409 DUPLICATE
        mvc.perform(put("/api/v1/silences").contentType("application/json")
                        .content(silenceBody("r-api", start, end)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value("DUPLICATE"));

        // 3) 区间重叠 → 409 OVERLAP
        mvc.perform(put("/api/v1/silences").contentType("application/json")
                        .content(silenceBody("r-api", start + 1_000, end + 1_000)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value("OVERLAP"));

        // 4) 非法区间 → 400 INVALID_RANGE
        mvc.perform(put("/api/v1/silences").contentType("application/json")
                        .content(silenceBody("r-api", end, start)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("INVALID_RANGE"));

        // 5) 整体落在过去 → 400 IN_PAST
        mvc.perform(put("/api/v1/silences").contentType("application/json")
                        .content(silenceBody("r-api", now - 20_000, now - 10_000)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("IN_PAST"));

        // 6) 查询：按规则过滤
        mvc.perform(get("/api/v1/silences").param("ruleId", "r-api"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].ruleId").value("r-api"));

        // 7) 取消：首次 CANCELLED，重复 NOT_FOUND，可区分
        mvc.perform(delete("/api/v1/silences/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
        mvc.perform(delete("/api/v1/silences/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("NOT_FOUND"));

        // 8) 通知台账 / 抖动快照 / 状态自观测接口可达
        mvc.perform(get("/api/v1/notifications"))
                .andExpect(status().isOk());
        mvc.perform(get("/api/v1/notifications/flapping"))
                .andExpect(status().isOk());
        mvc.perform(get("/api/v1/notifications/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.silences").value(0))
                .andExpect(jsonPath("$.flapTrackersEvicted").exists())
                .andExpect(jsonPath("$.cooldownTrackersEvicted").exists());
    }
}
