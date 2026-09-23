package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.DegradeReason;
import com.github.h3yow.observabilitypipelinejava.model.IngestStatus;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 基数/速率超限时降级拒绝（原因可区分），压力回落后恢复正常。 */
class DegradationTest {

    private static final Logger LOG = LoggerFactory.getLogger(DegradationTest.class);

    @Test
    @DisplayName("单指标基数超限拒绝并标记原因；已有序列与其他指标不受影响；驱逐后配额回落恢复")
    void metricCardinalityLimitAndRecovery() {
        TestHarness h = TestHarness.create(); // maxSeriesPerMetric=3, maxGlobal=10
        long now = h.now();

        for (int i = 0; i < 3; i++) {
            var r = h.send("cpu", Map.of("host", "h" + i), now, 1.0, "s");
            assertEquals(IngestStatus.ACCEPTED, r.status(), "host h" + i);
        }
        var overflow = h.send("cpu", Map.of("host", "h3"), now, 1.0, "s");
        LOG.info("4th series -> {} :: {}", overflow.status(), overflow.detail());
        assertEquals(IngestStatus.CARDINALITY_LIMIT, overflow.status());
        assertEquals(DegradeReason.METRIC_CARDINALITY_EXCEEDED, h.ingest.pressure().primaryReason());

        // 已有序列继续正常上报，不被误伤
        var existing = h.send("cpu", Map.of("host", "h0"), now, 2.0, "s");
        assertEquals(IngestStatus.ACCEPTED, existing.status());

        // 其他指标仍可接纳
        var otherMetric = h.send("mem", Map.of("host", "h9"), now, 1.0, "s");
        assertEquals(IngestStatus.ACCEPTED, otherMetric.status());

        // 压力可观测
        var pressure = h.ingest.pressure();
        LOG.info("PRESSURE globalCount={} perMetric={} metricLimited={} primary={}",
                pressure.currentSeriesCount(), pressure.seriesPerMetric(),
                pressure.metricCardinalityLimited(), pressure.primaryReason());
        assertEquals(4, pressure.currentSeriesCount());
        assertTrue(pressure.metricCardinalityLimited());

        // 时间推进超过 retention，tick 驱逐窗口并释放基数配额 -> 系统恢复
        h.advanceMs(120_000);
        var tick = h.tick();
        LOG.info("EVICT TICK evictedWindows={} deregisteredSeries={}",
                tick.evictedWindows(), tick.deregisteredSeries());
        assertEquals(4, tick.deregisteredSeries());
        var afterRecover = h.send("cpu", Map.of("host", "h3"), h.now(), 1.0, "s");
        LOG.info("after eviction h3 -> {}", afterRecover.status());
        assertEquals(IngestStatus.ACCEPTED, afterRecover.status());
        assertFalse(h.ingest.pressure().metricCardinalityLimited());
        assertEquals(DegradeReason.NONE, h.ingest.pressure().primaryReason());
    }

    @Test
    @DisplayName("全局基数超限与单指标超限可区分")
    void globalCardinalityLimit() {
        TestHarness h = TestHarness.create(); // perMetric=3, global=10
        long now = h.now();
        // 3 个指标 x 3 序列 = 9
        for (int m = 0; m < 3; m++) {
            for (int i = 0; i < 3; i++) {
                var r = h.send("m" + m, Map.of("host", "h" + i), now, 1.0, "s");
                assertEquals(IngestStatus.ACCEPTED, r.status());
            }
        }
        // 第 10 个序列落在 m0 会先撞 perMetric(=3)，所以用新指标 m3 验证全局上限
        var tenth = h.send("m3", Map.of("host", "h0"), now, 1.0, "s");
        assertEquals(IngestStatus.ACCEPTED, tenth.status());
        var eleventh = h.send("m3", Map.of("host", "h1"), now, 1.0, "s");
        LOG.info("11th global series -> {} :: {}", eleventh.status(), eleventh.detail());
        assertEquals(IngestStatus.GLOBAL_CARDINALITY_LIMIT, eleventh.status());
        assertEquals(DegradeReason.GLOBAL_CARDINALITY_EXCEEDED,
                h.ingest.pressure().primaryReason());
    }

    @Test
    @DisplayName("令牌桶速率超限拒绝，时钟推进后配额恢复")
    void rateLimitRecovers() {
        ObservabilityProperties p = TestHarness.defaultProps();
        p.setRateLimitPerSec(5);
        TestHarness h = TestHarness.create(p);
        long now = h.now();
        int accepted = 0;
        int limited = 0;
        int other = 0;
        for (int i = 0; i < 8; i++) {
            // 同一序列：第 2..8 条数值/时间相同会被判重，因此数值递增以隔离速率语义
            var r = h.send("cpu", Map.of("host", "h0"), now, i, "s");
            if (r.status() == IngestStatus.ACCEPTED) {
                accepted++;
            } else if (r.status() == IngestStatus.RATE_LIMITED) {
                limited++;
            } else {
                other++;
            }
        }
        LOG.info("burst accepted={} rateLimited={} other={}", accepted, limited, other);
        assertEquals(5, accepted);
        assertEquals(3, limited);
        assertEquals(0, other);

        // 时钟推进 1 秒，令牌回补，系统恢复接纳
        h.advanceMs(1_000);
        var recovered = h.send("cpu", Map.of("host", "h0"), h.now(), 99.0, "s");
        LOG.info("after 1s refill -> {}", recovered.status());
        assertEquals(IngestStatus.ACCEPTED, recovered.status());

    }
}
