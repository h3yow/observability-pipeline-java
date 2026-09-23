package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.domain.*;
import com.github.h3yow.observabilitypipelinejava.window.WindowMath;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 维护推进测试：仅靠时钟前进 + tick，自动按窗口顺序完成封板评估，
 * 跳过的多个窗口也会被补齐；同一输入下事件结果可复现。
 */
public class MaintenanceTickTest {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceTickTest.class);

    @Test
    void tickEvaluatesSealedWindowsInOrder() {
        PipelineHarness h = new PipelineHarness();
        long ws0 = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        h.clock.setMillis(ws0 + 1_000L);
        h.engine.addRule(new AlertRule("r", "cpu", Aggregation.AVG, ComparisonOp.GT, 50d, 1, Map.of()));

        // 连续 3 个窗口：高、低、高
        long[] wss = {ws0, ws0 + 60_000L, ws0 + 120_000L};
        double[] vals = {60d, 10d, 70d};
        for (int i = 0; i < 3; i++) {
            h.clock.setMillis(wss[i] + 1_000L);
            IngestResult r = h.ingest.ingest(new MetricSample("cpu", Map.of("host", "a"),
                    vals[i], wss[i] + 500L));
            assertEquals(IngestStatus.ACCEPTED, r.status(), r.detail());
        }

        // 一次 tick 直接跨过三个窗口的封板时刻
        h.clock.setMillis(ws0 + 3 * 60_000L + h.props.getGracePeriodMillis() + 1L);
        int n = h.maintenance.tick();
        log.info("tick evaluated {} sealed windows", n);
        assertTrue(n >= 3, "应补齐至少 3 个已封板窗口");

        List<AlertEvent> events = h.engine.events("r");
        log.info("auto-evaluated events (in order):");
        for (AlertEvent e : events) {
            log.info("  seq={} type={} window={} observed={}",
                    e.eventSeq(), e.type(), e.windowStart(), e.observedValue());
        }
        assertEquals(3, events.size());
        assertEquals(AlertEventType.FIRING, events.get(0).type());
        assertEquals(AlertEventType.RESOLVED, events.get(1).type());
        assertEquals(AlertEventType.FIRING, events.get(2).type());
        assertEquals(wss[0], events.get(0).windowStart());
        assertEquals(wss[1], events.get(1).windowStart());
        assertEquals(wss[2], events.get(2).windowStart());

        // 幂等：再次 tick 不产生重复事件
        int n2 = h.maintenance.tick();
        log.info("second tick evaluated {} windows (no new events expected)", n2);
        assertEquals(3, h.engine.events("r").size(), "同窗口重复推进必须幂等");

        // 可复现：相同输入序列 -> 相同事件序列（再搭一条管线重放）
        PipelineHarness h2 = new PipelineHarness();
        h2.clock.setMillis(ws0 + 1_000L);
        h2.engine.addRule(new AlertRule("r", "cpu", Aggregation.AVG, ComparisonOp.GT, 50d, 1, Map.of()));
        for (int i = 0; i < 3; i++) {
            h2.clock.setMillis(wss[i] + 1_000L);
            h2.ingest.ingest(new MetricSample("cpu", Map.of("host", "a"), vals[i], wss[i] + 500L));
        }
        h2.clock.setMillis(ws0 + 3 * 60_000L + h2.props.getGracePeriodMillis() + 1L);
        h2.maintenance.tick();
        List<AlertEvent> events2 = h2.engine.events("r");
        assertEquals(events.size(), events2.size());
        for (int i = 0; i < events.size(); i++) {
            assertEquals(events.get(i).type(), events2.get(i).type());
            assertEquals(events.get(i).windowStart(), events2.get(i).windowStart());
            assertEquals(events.get(i).observedValue(), events2.get(i).observedValue(), 0d);
        }
        log.info("replayed pipeline produced identical event sequence ({} events)", events2.size());
    }
}
