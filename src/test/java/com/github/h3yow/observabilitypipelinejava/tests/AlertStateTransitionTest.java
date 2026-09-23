package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.model.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.AlertState;
import com.github.h3yow.observabilitypipelinejava.model.AlertStateSnapshot;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 告警状态机迁移（显式时间线，结果可复现）：
 * INACTIVE -> PENDING -> FIRING -> RECOVERED -> (再次) FIRING；
 * 条件未持续不得误报；FIRING 内重复命中按维度去重；恢复后再触发 occurrence 递增。
 */
class AlertStateTransitionTest {

    private static final Logger LOG = LoggerFactory.getLogger(AlertStateTransitionTest.class);

    private static final long W = 10_000;
    private static final long GRACE = 20_000;

    private AlertStateSnapshot state(TestHarness h, String ruleId, String fp) {
        return h.alertStates.get(h.alertStates.keyOf(ruleId, fp));
    }

    private String fp(String host) {
        return com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                .fingerprint(com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                        .normalize(Map.of("host", host)));
    }

    /**
     * 驱动窗口 i：在其开放期上报该窗口样本，然后把时钟推进到该窗口可封存并 tick。
     * 关键：上报始终发生在“当前开放窗口”，避免自造迟到/封存竞态。
     */
    private void drive(TestHarness h, String host, int windowIndex, double value) {
        long targetWindow = h.timing.windowStartFor(h.now()) + windowIndex * W;
        // 时钟对齐到目标窗口的开放期（窗口起点后 1 秒）。
        long desiredNow = targetWindow + 1_000;
        if (desiredNow > h.now()) {
            h.advanceMs(desiredNow - h.now());
        }
        h.send("latency", Map.of("host", host), targetWindow + 1_000, value, "probe");
        // 推进到 targetWindow + W + GRACE + 1，使该窗口恰好可被封存。
        long sealAt = targetWindow + W + GRACE + 1;
        h.advanceMs(sealAt - h.now());
        h.tick();
    }

    @Test
    @DisplayName("fireAfterWindows=2：单窗口命中只到 PENDING 不告警；中断后清零，不误报")
    void pendingDoesNotFireAndInterruptResets() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 2));

        drive(h, "a", 0, 150);
        var after1 = state(h, "r1", fp("a"));
        LOG.info("W0 state={} consecutive={}", after1.state(), after1.consecutiveMatchedWindows());
        assertEquals(AlertState.PENDING, after1.state());
        assertEquals(1, after1.consecutiveMatchedWindows());
        assertTrue(h.alertStates.events("r1").stream()
                .noneMatch(e -> e.type() == AlertEventType.FIRED));

        drive(h, "a", 1, 50);
        var after2 = state(h, "r1", fp("a"));
        LOG.info("W1 state={} consecutive={}", after2.state(), after2.consecutiveMatchedWindows());
        assertEquals(AlertState.INACTIVE, after2.state());
        assertEquals(0, after2.consecutiveMatchedWindows());

        drive(h, "a", 2, 150);
        var after3 = state(h, "r1", fp("a"));
        LOG.info("W2 state={} consecutive={}", after3.state(), after3.consecutiveMatchedWindows());
        assertEquals(AlertState.PENDING, after3.state());
        assertEquals(1, after3.consecutiveMatchedWindows());
        assertTrue(h.alertStates.events("r1").stream()
                .noneMatch(e -> e.type() == AlertEventType.FIRED));
    }

    @Test
    @DisplayName("持续满足触发；FIRING 重复命中不堆事件；恢复后再触发产生可区分新事件")
    void fireDedupeRecoverRefire() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 2));

        drive(h, "a", 0, 150);
        assertEquals(AlertState.PENDING, state(h, "r1", fp("a")).state());
        drive(h, "a", 1, 160);
        var firing = state(h, "r1", fp("a"));
        assertEquals(AlertState.FIRING, firing.state());
        assertEquals(1, firing.fireOccurrence());

        drive(h, "a", 2, 170);
        var stillFiring = state(h, "r1", fp("a"));
        assertEquals(AlertState.FIRING, stillFiring.state());
        assertEquals(1, stillFiring.fireOccurrence());
        long firedCount = h.alertStates.events("r1").stream()
                .filter(e -> e.type() == AlertEventType.FIRED).count();
        assertEquals(1, firedCount);
        LOG.info("FIRING repeated window -> firedEventCount={} (deduplicated)", firedCount);

        drive(h, "a", 3, 10);
        assertEquals(AlertState.RECOVERED, state(h, "r1", fp("a")).state());

        drive(h, "a", 4, 200);
        var mid = state(h, "r1", fp("a"));
        LOG.info("W4 after recover state={} occurrence={}", mid.state(), mid.fireOccurrence());
        assertEquals(AlertState.PENDING, mid.state());

        drive(h, "a", 5, 210);
        var refired = state(h, "r1", fp("a"));
        assertEquals(AlertState.FIRING, refired.state());
        assertEquals(2, refired.fireOccurrence());

        List<AlertEvent> firedEvents = h.alertStates.events("r1").stream()
                .filter(e -> e.type() == AlertEventType.FIRED).toList();
        LOG.info("FIRED events: {}", firedEvents);
        assertEquals(2, firedEvents.size());
        assertNotEquals(firedEvents.get(0).eventId(), firedEvents.get(1).eventId());
        assertEquals(1, firedEvents.get(0).fireOccurrence());
        assertEquals(2, firedEvents.get(1).fireOccurrence());

        List<AlertEvent> recoveredEvents = h.alertStates.events("r1").stream()
                .filter(e -> e.type() == AlertEventType.RECOVERED).toList();
        assertEquals(1, recoveredEvents.size());
        assertTrue(firedEvents.get(0).windowStartMs() < recoveredEvents.get(0).windowStartMs());
        assertTrue(recoveredEvents.get(0).windowStartMs() < firedEvents.get(1).windowStartMs());
    }

    @Test
    @DisplayName("恢复后再观察一个判定周期：继续不命中则静默回 INACTIVE，不重复产生 RECOVERED")
    void recoveredSettlesToInactive() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        drive(h, "a", 0, 150); // FIRING occurrence=1
        drive(h, "a", 1, 10);  // RECOVERED
        drive(h, "a", 2, 10);  // 静默回到 INACTIVE，不产生第二个 RECOVERED
        var settled = state(h, "r1", fp("a"));
        LOG.info("W2 state={} occurrence={}", settled.state(), settled.fireOccurrence());
        assertEquals(AlertState.INACTIVE, settled.state());

        long recoveredCount = h.alertStates.events("r1").stream()
                .filter(e -> e.type() == AlertEventType.RECOVERED).count();
        assertEquals(1, recoveredCount);
    }

    @Test
    @DisplayName("同一规则按标签维度独立判定、独立去重")
    void dimensionsAreIndependent() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        // 窗口0：两维度同时上报
        long w0 = h.timing.windowStartFor(h.now());
        h.send("latency", Map.of("host", "a"), w0 + 1_000, 150, "probe");
        h.send("latency", Map.of("host", "b"), w0 + 1_000, 50, "probe");
        h.advanceMs(W + GRACE + 1);
        h.tick();
        assertEquals(AlertState.FIRING, state(h, "r1", fp("a")).state());
        assertEquals(AlertState.INACTIVE, state(h, "r1", fp("b")).state());

        long w1 = h.timing.windowStartFor(h.now());
        h.send("latency", Map.of("host", "a"), w1 + 1_000, 160, "probe");
        h.send("latency", Map.of("host", "b"), w1 + 1_000, 120, "probe");
        h.advanceMs(W + GRACE + 1);
        h.tick();
        assertEquals(AlertState.FIRING, state(h, "r1", fp("a")).state());
        assertEquals(AlertState.FIRING, state(h, "r1", fp("b")).state());

        long firedEvents = h.alertStates.events("r1").stream()
                .filter(e -> e.type() == AlertEventType.FIRED).count();
        LOG.info("Per-dimension fired event count={} (expect 2, not 3)", firedEvents);
        assertEquals(2, firedEvents);
    }
}
