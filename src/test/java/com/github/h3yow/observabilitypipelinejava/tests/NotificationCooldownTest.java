package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import com.github.h3yow.observabilitypipelinejava.model.NotificationDecision;
import com.github.h3yow.observabilitypipelinejava.model.NotificationKind;
import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通知冷却与去重：同一「规则 × 维度」下发触发通知后进入冷却期；
 * 冷却期内的重复触发被抑制并携带所属轮次；冷却结束仍触发则再发一轮
 * （轮次递增，与被抑制的重复项一眼可辨）。恢复通知不受冷却影响。
 */
class NotificationCooldownTest {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationCooldownTest.class);

    private static final Map<String, String> HOST_A = Map.of("host", "a");
    private static final Map<String, String> HOST_B = Map.of("host", "b");

    /** 窗口评估时刻间隔约 40~80s，冷却 100s 覆盖 T1/T2，T3 时到期。 */
    private static final long COOLDOWN = 100_000;

    private int logged;

    private TestHarness cooldownHarness() {
        ObservabilityProperties p = TestHarness.defaultProps();
        p.setNotificationCooldownMs(COOLDOWN);
        return TestHarness.create(p);
    }

    private void logNew(TestHarness h, String ruleId) {
        List<NotificationRecord> all = h.notifications.list(ruleId);
        for (int i = logged; i < all.size(); i++) {
            NotificationRecord r = all.get(i);
            LOG.info("NOTIFY kind={} decision={} round={} occurrence={} windowStart={} src={} detail={}",
                    r.kind(), r.decision(), r.notificationRound(), r.fireOccurrence(),
                    r.windowStartMs(), r.sourceEventId(), r.detail());
        }
        logged = all.size();
    }

    private List<NotificationRecord> delivered(TestHarness h, NotificationKind kind) {
        return h.notifications.list("r1").stream()
                .filter(r -> r.kind() == kind && r.decision() == NotificationDecision.DELIVERED)
                .toList();
    }

    @Test
    @DisplayName("冷却期内再次触发被抑制；冷却结束仍触发则发出新一轮可区分通知；恢复不受冷却影响")
    void cooldownSuppressesRefireThenNewRoundAfterExpiry() {
        TestHarness h = cooldownHarness();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        h.driveWindow("latency", HOST_A, 0, 150); // T0: FIRED 下发 round=1
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 1, 10);  // T1: RECOVERED 照常下发（不受冷却影响）
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 2, 150); // T2: 再次触发，冷却中 → 抑制
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 3, 150); // T3: 冷却到期仍触发 → 新一轮 round=2
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 4, 10);  // T4: 恢复照常下发
        logNew(h, "r1");

        List<NotificationRecord> fired = delivered(h, NotificationKind.FIRED);
        assertEquals(2, fired.size(), "冷却期内的触发被抑制，全程只下发两轮");
        assertEquals(1, fired.get(0).notificationRound());
        assertEquals(2, fired.get(1).notificationRound());
        assertTrue(fired.stream().map(NotificationRecord::notificationId).distinct().count() == 2,
                "两轮通知 ID 可区分");

        List<NotificationRecord> suppressed = h.notifications.list("r1").stream()
                .filter(r -> r.decision() == NotificationDecision.SUPPRESSED_COOLDOWN).toList();
        assertEquals(1, suppressed.size());
        assertEquals(1, suppressed.get(0).notificationRound(),
                "被抑制的重复项携带所属轮次 round=1，与新一轮 round=2 可区分");
        assertEquals(2, suppressed.get(0).fireOccurrence());

        assertEquals(2, delivered(h, NotificationKind.RECOVERED).size(),
                "两次恢复都不受冷却影响，照常下发");
    }

    @Test
    @DisplayName("持续触发不逐次刷屏：每个冷却周期至多补发一轮")
    void stillFiringRepeatsOncePerCooldown() {
        TestHarness h = cooldownHarness();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        for (int i = 0; i < 5; i++) {
            h.driveWindow("latency", HOST_A, i, 150); // 持续满足
            logNew(h, "r1");
        }

        List<NotificationRecord> fired = delivered(h, NotificationKind.FIRED);
        assertEquals(2, fired.size(), "T0 下发首轮；T3 冷却到期补发一轮；其余窗口不逐次发");
        assertEquals(1, fired.get(0).notificationRound());
        assertEquals(2, fired.get(1).notificationRound());
        assertEquals(null, fired.get(1).sourceEventId(), "冷却到期补发由对账产生，无源事件");
        assertTrue(fired.get(1).detail().contains("still firing"));
        assertEquals(1, fired.get(0).fireOccurrence());
        assertEquals(1, fired.get(1).fireOccurrence(), "同一状态机轮次内的重复通知");
    }

    @Test
    @DisplayName("跟踪器容量超限：不崩溃、不静默，降级为可区分的 DELIVERED_UNTRACKED")
    void trackerCapacityDeliversUntracked() {
        ObservabilityProperties p = TestHarness.defaultProps();
        p.setNotificationCooldownMs(COOLDOWN);
        p.setNotificationMaxTrackers(1);
        TestHarness h = TestHarness.create(p);
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        long w0 = h.timing.windowStartFor(h.now());
        h.send("latency", HOST_A, w0 + 1_000, 150, "probe");
        h.send("latency", HOST_B, w0 + 1_000, 160, "probe");
        h.advanceMs(w0 + 30_001 - h.now());
        h.tick();
        logNew(h, "r1");

        List<NotificationRecord> all = h.notifications.list("r1");
        assertEquals(1, all.stream()
                .filter(r -> r.decision() == NotificationDecision.DELIVERED).count(),
                "容量内的一个维度正常下发");
        assertEquals(1, all.stream()
                .filter(r -> r.decision() == NotificationDecision.DELIVERED_UNTRACKED).count(),
                "超限维度降级下发且结果可区分");
        assertEquals(1, h.notificationGate.trackerCount(), "跟踪器数量不超过上限");
    }
}
