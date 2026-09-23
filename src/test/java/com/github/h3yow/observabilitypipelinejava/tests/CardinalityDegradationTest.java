package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.domain.IngestResult;
import com.github.h3yow.observabilitypipelinejava.domain.IngestStatus;
import com.github.h3yow.observabilitypipelinejava.domain.MetricSample;
import com.github.h3yow.observabilitypipelinejava.domain.RejectReason;
import com.github.h3yow.observabilitypipelinejava.window.WindowMath;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 标签基数超限：拒绝并进入可区分降级状态；窗口过期回收、计数回落后恢复受理。
 */
public class CardinalityDegradationTest {

    private static final Logger log = LoggerFactory.getLogger(CardinalityDegradationTest.class);

    @Test
    void perMetricLimitRejectsAndRecoversAfterEviction() {
        PipelineHarness h = new PipelineHarness(); // per-metric=3, global=10
        long ws0 = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        h.clock.setMillis(ws0 + 30_000L);

        for (int i = 0; i < 3; i++) {
            IngestResult r = h.ingest.ingest(
                    new MetricSample("m", Map.of("id", String.valueOf(i)), 1d, ws0 + 1_000 + i));
            log.info("accept series id={} => {} {}", i, r.status(), r.reason());
            assertEquals(IngestStatus.ACCEPTED, r.status());
        }
        IngestResult over = h.ingest.ingest(
                new MetricSample("m", Map.of("id", "boom"), 1d, ws0 + 5_000));
        log.info("4th series => status={} reason={} detail={}", over.status(), over.reason(), over.detail());
        assertEquals(IngestStatus.REJECTED, over.status());
        assertEquals(RejectReason.CARDINALITY_LIMIT, over.reason());
        assertTrue(h.stats.degradation().containsKey(
                com.github.h3yow.observabilitypipelinejava.service.PipelineStats.DEGRADE_CARDINALITY_PER_METRIC));
        log.info("degradation after overflow => {}", h.stats.degradation());

        // 已存在序列在高基数压力下仍可写入（不影响存活流量），窗口数不受影响
        IngestResult existing = h.ingest.ingest(
                new MetricSample("m", Map.of("id", "0"), 2d, ws0 + 6_000));
        assertEquals(IngestStatus.ACCEPTED, existing.status());

        // 推进超过保留期并 tick：旧窗口回收 -> 序列计数释放 -> 降级清除 -> 新序列恢复受理
        h.clock.setMillis(ws0 + h.props.getRetentionMillis() + 2 * h.props.getWindowSizeMillis());
        int windows = h.maintenance.tick();
        log.info("tick after retention => evaluatedWindows={} activeSeries={} activeForMetric={} degradation={}",
                windows, h.store.activeSeriesCount(), h.store.activeSeriesCountForMetric("m"),
                h.stats.degradation());
        assertEquals(0, h.store.activeSeriesCount());
        assertFalse(h.stats.degradation().containsKey(
                com.github.h3yow.observabilitypipelinejava.service.PipelineStats.DEGRADE_CARDINALITY_PER_METRIC),
                "压力回落后必须恢复正常状态");

        long nowWs = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        IngestResult recovered = h.ingest.ingest(
                new MetricSample("m", Map.of("id", "new-after-recovery"), 1d, nowWs + 1_000));
        log.info("new series after recovery => status={} reason={}", recovered.status(), recovered.reason());
        assertEquals(IngestStatus.ACCEPTED, recovered.status());
    }

    @Test
    void globalLimitGivesDistinctReason() {
        com.github.h3yow.observabilitypipelinejava.config.PipelineProperties p =
                PipelineHarness.defaultProps();
        p.setMaxSeriesPerMetric(100); // 放大单指标上限，让 global=10 先生效
        PipelineHarness h = new PipelineHarness(p);
        long ws = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        h.clock.setMillis(ws + 30_000L);
        // 不同指标各 1 个序列，共 10 个后再上报第 11 个
        for (int i = 0; i < 10; i++) {
            IngestResult r = h.ingest.ingest(new MetricSample("g" + i, Map.of("id", "x"),
                    1d, ws + 1_000));
            assertEquals(IngestStatus.ACCEPTED, r.status(), r.detail());
        }
        IngestResult over = h.ingest.ingest(new MetricSample("g10", Map.of("id", "x"),
                1d, ws + 1_000));
        log.info("11th global series => status={} reason={}", over.status(), over.reason());
        assertEquals(RejectReason.GLOBAL_CARDINALITY_LIMIT, over.reason());
        assertTrue(h.stats.degradation().containsKey(
                com.github.h3yow.observabilitypipelinejava.service.PipelineStats.DEGRADE_CARDINALITY_GLOBAL));
    }
}
