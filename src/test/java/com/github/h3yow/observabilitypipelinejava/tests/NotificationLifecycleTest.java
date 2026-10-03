package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import com.github.h3yow.observabilitypipelinejava.model.SilenceCreateStatus;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通知控制状态的回收：规则删除即刻回收；维度老化随窗口推进回收；
 * 跟踪条目到达可控上限时淘汰最旧并计数（可观测，不静默失效、不崩溃）。
 */
class NotificationLifecycleTest {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationLifecycleTest.class);

    private static final long W = 10_000;
    private static final long GRACE = 20_000;

    private void drive(TestHarness h, String host, int windowIndex, double value) {
        long targetWindow = h.timing.windowStartFor(h.now()) + windowIndex * W;
        long desiredNow = targetWindow + 1_000;
        if (desiredNow > h.now()) {
            h.advanceMs(desiredNow - h.now());
        }
        h.send("latency", Map.of("host", host), targetWindow + 1_000, value, "probe");
        long sealAt = targetWindow + W + GRACE + 1;
        h.advanceMs(sealAt - h.now());
        h.tick();
    }

    @Test
    @DisplayName("规则删除即刻回收其静默、冷却与抖动跟踪状态")
    void ruleDeletionReclaimsNotificationState() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));
        long w0 = h.timing.windowStartFor(h.now());
        assertEquals(SilenceCreateStatus.CREATED,
                h.silences.create("r1", Map.of(), w0 + 200_000, w0 + 300_000, "m", h.now())
                        .status());

        drive(h, "a", 0, 150); // 触发：产生冷却与抖动跟踪条目
        LOG.info("BEFORE DELETE silences={} flap={} cooldown={}",
                h.silences.size(), h.flapTracker.size(), h.notifications.cooldownSize());
        assertEquals(1, h.silences.size());
        assertEquals(1, h.flapTracker.size());
        assertEquals(1, h.notifications.cooldownSize());

        h.rules.remove("r1");
        h.notifications.onRuleDeleted("r1");
        LOG.info("AFTER DELETE silences={} flap={} cooldown={}",
                h.silences.size(), h.flapTracker.size(), h.notifications.cooldownSize());
        assertEquals(0, h.silences.size(), "规则删除后静默应回收");
        assertEquals(0, h.flapTracker.size(), "规则删除后抖动跟踪应回收");
        assertEquals(0, h.notifications.cooldownSize(), "规则删除后冷却跟踪应回收");
    }

    @Test
    @DisplayName("维度停止上报后，冷却与抖动跟踪随窗口推进老化回收")
    void trackersAgedOutAfterDimensionInactive() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        drive(h, "a", 0, 150);
        assertEquals(1, h.flapTracker.size());
        assertEquals(1, h.notifications.cooldownSize());

        // 维度不再上报：推进时钟超过老化阈值（retention=100s，冷却=120s），空 tick 也会回收。
        h.advanceMs(500_000);
        h.tick();
        LOG.info("AFTER AGING flap={} cooldown={}",
                h.flapTracker.size(), h.notifications.cooldownSize());
        assertEquals(0, h.flapTracker.size(), "老化后抖动跟踪应回收");
        assertEquals(0, h.notifications.cooldownSize(), "老化后冷却跟踪应回收");
    }

    @Test
    @DisplayName("跟踪条目达到上限：淘汰最旧并计数，不崩溃也不静默失效")
    void trackerLimitEvictsOldestAndCounts() {
        ObservabilityProperties props = TestHarness.defaultProps();
        props.setMaxNotificationTrackers(2);
        TestHarness h = TestHarness.create(props);
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        // 同一窗口 3 个维度同时触发，跟踪上限为 2。
        long w0 = h.timing.windowStartFor(h.now());
        h.send("latency", Map.of("host", "a"), w0 + 1_000, 150, "probe");
        h.send("latency", Map.of("host", "b"), w0 + 1_000, 160, "probe");
        h.send("latency", Map.of("host", "c"), w0 + 1_000, 170, "probe");
        h.advanceMs(W + GRACE + 1);
        h.tick();

        LOG.info("LIMIT flap size={} evicted={} | cooldown size={} evicted={}",
                h.flapTracker.size(), h.flapTracker.evictedCount(),
                h.notifications.cooldownSize(), h.notifications.cooldownEvictedCount());
        assertEquals(2, h.flapTracker.size(), "抖动跟踪不超过上限");
        assertTrue(h.flapTracker.evictedCount() >= 1, "淘汰必须计数、可观测");
        assertEquals(2, h.notifications.cooldownSize(), "冷却跟踪不超过上限");
        assertTrue(h.notifications.cooldownEvictedCount() >= 1, "淘汰必须计数、可观测");
    }
}
