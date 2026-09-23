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
 * 告警状态机迁移测试：
 * 未触发 -> 待确认 -> 触发 -> 恢复；条件不持续满足不得误报；
 * 同维度重复触发去重；恢复后再触发产生可区分的新事件。
 */
public class AlertStateMachineTest {

    private static final Logger log = LoggerFactory.getLogger(AlertStateMachineTest.class);

    private final PipelineHarness h = new PipelineHarness();

    private void sample(String metric, Map<String, String> labels, double v, long ts) {
        IngestResult r = h.ingest.ingest(new MetricSample(metric, labels, v, ts));
        assertNotEquals(IngestStatus.REJECTED, r.status(), () -> "sample rejected: " + r.detail());
    }

    private List<AlertEvent> closeWindowAndEvaluate(long ws) {
        // 先把时钟放到窗口末尾，保证该窗口后续补样本不会被判未来；
        // 再推进到 窗口结束 + 宽限期，保证封板
        h.clock.setMillis(ws + h.props.getWindowSizeMillis() - 1L);
        h.clock.setMillis(ws + h.props.getWindowSizeMillis() + h.props.getGracePeriodMillis() + 1L);
        List<AlertEvent> events = h.engine.evaluateWindow("cpu", ws, h.props.getWindowSizeMillis());
        for (AlertEvent e : events) {
            log.info("EVENT seq={} rule={} dim={} type={} window={} observed={} threshold={} | {}",
                    e.eventSeq(), e.ruleId(), e.labels(), e.type().name(),
                    e.windowStart(), e.observedValue(), e.threshold(), e.reason());
        }
        log.info("states after window {} => {}", ws, h.engine.stateSnapshot());
        return events;
    }

    @Test
    void fullLifecyclePendingFiringDedupResolvedRefire() {
        long ws0 = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        h.clock.setMillis(ws0 + 30_000L);
        AlertRule rule = new AlertRule("r1", "cpu", Aggregation.AVG, ComparisonOp.GT, 50d, 2, Map.of());
        h.engine.addRule(rule);
        log.info("rule added: {}", rule);

        // 窗口1：满足一次 -> PENDING，无事件（条件未持续满足，不能误报）
        sample("cpu", Map.of("host", "a"), 60d, ws0 + 1_000);
        List<AlertEvent> e1 = closeWindowAndEvaluate(ws0);
        assertTrue(e1.isEmpty(), "首次满足只应进入 PENDING");
        assertEquals("PENDING", h.engine.stateSnapshot().get("r1").get("cpu{host=a}"));

        // 窗口2：条件不满足 -> 回 INACTIVE（误报防护）
        long ws1 = ws0 + h.props.getWindowSizeMillis();
        sample("cpu", Map.of("host", "a"), 10d, ws1 + 1_000);
        List<AlertEvent> e2 = closeWindowAndEvaluate(ws1);
        assertTrue(e2.isEmpty(), "PENDING 中断不得产生 FIRING");
        assertEquals("INACTIVE", h.engine.stateSnapshot().get("r1").get("cpu{host=a}"));

        // 窗口3、4：连续两次满足 -> 窗口4 产生 FIRING
        long ws2 = ws1 + h.props.getWindowSizeMillis();
        sample("cpu", Map.of("host", "a"), 70d, ws2 + 1_000);
        closeWindowAndEvaluate(ws2);
        assertEquals("PENDING", h.engine.stateSnapshot().get("r1").get("cpu{host=a}"));

        long ws3 = ws2 + h.props.getWindowSizeMillis();
        sample("cpu", Map.of("host", "a"), 80d, ws3 + 1_000);
        List<AlertEvent> e4 = closeWindowAndEvaluate(ws3);
        assertEquals(1, e4.size());
        assertEquals(AlertEventType.FIRING, e4.get(0).type());
        assertEquals("FIRING", h.engine.stateSnapshot().get("r1").get("cpu{host=a}"));

        // 窗口5：仍满足 -> 不重复产生 FIRING（维度去重）
        long ws4 = ws3 + h.props.getWindowSizeMillis();
        sample("cpu", Map.of("host", "a"), 90d, ws4 + 1_000);
        List<AlertEvent> e5 = closeWindowAndEvaluate(ws4);
        assertTrue(e5.isEmpty(), "FIRING 状态下同维度重复满足不得堆积事件");

        // 窗口6：不满足 -> RESOLVED 事件，状态 RESOLVED
        long ws5 = ws4 + h.props.getWindowSizeMillis();
        sample("cpu", Map.of("host", "a"), 1d, ws5 + 1_000);
        List<AlertEvent> e6 = closeWindowAndEvaluate(ws5);
        assertEquals(1, e6.size());
        assertEquals(AlertEventType.RESOLVED, e6.get(0).type());
        long firingSeq = e4.get(0).eventSeq();
        long resolvedSeq = e6.get(0).eventSeq();
        assertTrue(resolvedSeq > firingSeq);

        // 窗口7：再次连续满足两个窗口后重新 FIRING，事件序号与第一次 FIRING 可区分
        long ws6 = ws5 + h.props.getWindowSizeMillis();
        sample("cpu", Map.of("host", "a"), 66d, ws6 + 1_000);
        closeWindowAndEvaluate(ws6); // RESOLVED -> INACTIVE -> PENDING
        long ws7 = ws6 + h.props.getWindowSizeMillis();
        sample("cpu", Map.of("host", "a"), 66d, ws7 + 1_000);
        List<AlertEvent> e8 = closeWindowAndEvaluate(ws7);
        assertEquals(1, e8.size());
        assertEquals(AlertEventType.FIRING, e8.get(0).type());
        long secondFiringSeq = e8.get(0).eventSeq();
        log.info("first firing seq={}, resolved seq={}, second firing seq={}",
                firingSeq, resolvedSeq, secondFiringSeq);
        assertTrue(secondFiringSeq > resolvedSeq, "恢复后再次触发必须是新的、可区分事件");

        List<AlertEvent> all = h.engine.events("r1");
        assertEquals(3, all.size(), "整个生命周期恰好 3 个事件：FIRING/RESOLVED/FIRING");
    }

    @Test
    void dimensionsAreDeduplicatedIndependently() {
        long ws0 = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        AlertRule rule = new AlertRule("r2", "cpu", Aggregation.AVG, ComparisonOp.GT, 50d, 1, Map.of());
        h.engine.addRule(rule);

        // host=a 与 host=b 同时满足，应各产生一个 FIRING（按维度分别去重）
        sample("cpu", Map.of("host", "a"), 60d, ws0 + 1_000);
        sample("cpu", Map.of("host", "b"), 70d, ws0 + 1_000);
        List<AlertEvent> events = closeWindowAndEvaluate(ws0);
        assertEquals(2, events.size());
        assertEquals(2, events.stream().filter(e -> e.type() == AlertEventType.FIRING).count());

        // 下一窗口两维度仍满足：不新增事件
        long ws1 = ws0 + h.props.getWindowSizeMillis();
        sample("cpu", Map.of("host", "a"), 61d, ws1 + 1_000);
        sample("cpu", Map.of("host", "b"), 71d, ws1 + 1_000);
        List<AlertEvent> again = closeWindowAndEvaluate(ws1);
        assertTrue(again.isEmpty(), "同维度持续触发应被去重");
        log.info("events total after two windows => {} (expected 2)", h.engine.events("r2").size());
        assertEquals(2, h.engine.events("r2").size());
    }
}
