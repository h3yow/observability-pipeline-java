package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import com.github.h3yow.observabilitypipelinejava.model.NotificationDecision;
import com.github.h3yow.observabilitypipelinejava.model.NotificationOrigin;
import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通知冷却与去重：FIRED 通知后进入冷却期，期内不逐次通知；
 * 冷却结束仍在触发补一条可区分的 REPEAT；恢复通知不受冷却影响；
 * 同样的输入序列结论可复现。
 *
 * <p>注意：驱动器每个窗口下标实际跨越的窗口起点间隔为 40s、50s、60s…（递增），
 * 冷却阈值据此选取，覆盖"期内/期满"两种情形。</p>
 */
class NotificationCooldownTest {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationCooldownTest.class);

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

    private TestHarness harnessWithCooldown(long cooldownMs) {
        ObservabilityProperties props = TestHarness.defaultProps();
        props.setNotificationCooldownMs(cooldownMs);
        return TestHarness.create(props);
    }

    private void logRecords(String tag, List<NotificationRecord> records) {
        for (NotificationRecord r : records) {
            LOG.info("{} kind={} origin={} decision={} occurrence={} round={} window={} id={}",
                    tag, r.kind(), r.origin(), r.decision(), r.fireOccurrence(),
                    r.notifyRound(), r.windowStartMs(), r.notificationId());
        }
    }

    @Test
    @DisplayName("持续触发：冷却期内不逐次通知，期满后补一条 REPEAT（轮次递增可区分）")
    void continuousFiringRepeatsOnlyAfterCooldown() {
        TestHarness h = harnessWithCooldown(70_000);
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        drive(h, "a", 0, 150); // w0+0s:   FIRED INITIAL round0
        drive(h, "a", 1, 160); // w0+40s:  距上次通知 40s < 70s，无通知
        drive(h, "a", 2, 170); // w0+90s:  距上次通知 90s >= 70s，REPEAT round1
        drive(h, "a", 3, 180); // w0+150s: 距上次通知 60s < 70s，无通知

        List<NotificationRecord> records = h.notifications.records("r1");
        logRecords("COOLDOWN", records);
        assertEquals(2, records.size(), "持续触发只产生 INITIAL + 一条 REPEAT");
        assertEquals(NotificationOrigin.INITIAL, records.get(0).origin());
        assertEquals(0, records.get(0).notifyRound());
        assertEquals(NotificationOrigin.REPEAT, records.get(1).origin());
        assertEquals(1, records.get(1).notifyRound());
        assertEquals(records.get(0).fireOccurrence(), records.get(1).fireOccurrence());
        assertNotEquals(records.get(0).notificationId(), records.get(1).notificationId(),
                "同一轮首次通知与冷却后再通知必须可区分");
    }

    @Test
    @DisplayName("冷却期内的再触发被抑制但记录在案；恢复通知不受冷却影响")
    void refireWithinCooldownSuppressedRecoveryNot() {
        TestHarness h = harnessWithCooldown(100_000);
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        drive(h, "a", 0, 150); // w0+0s:   FIRED occ1 NOTIFIED
        drive(h, "a", 1, 10);  // w0+40s:  RECOVERED NOTIFIED（恢复不受冷却影响）
        drive(h, "a", 2, 150); // w0+90s:  FIRED occ2，距上次通知 90s < 100s → 冷却抑制
        drive(h, "a", 3, 160); // w0+150s: 仍 FIRING，距上次通知 150s >= 100s → REPEAT

        List<NotificationRecord> records = h.notifications.records("r1");
        logRecords("COOLDOWN-REFIRE", records);

        NotificationRecord recovery = records.stream()
                .filter(r -> r.kind() == AlertEventType.RECOVERED).findFirst().orElseThrow();
        assertEquals(NotificationDecision.NOTIFIED, recovery.decision(),
                "恢复通知不受冷却影响，该出就出");

        NotificationRecord suppressedFire = records.stream()
                .filter(r -> r.kind() == AlertEventType.FIRED
                        && r.origin() == NotificationOrigin.INITIAL
                        && r.fireOccurrence() == 2).findFirst().orElseThrow();
        assertEquals(NotificationDecision.SUPPRESSED_BY_COOLDOWN, suppressedFire.decision());

        NotificationRecord repeat = records.stream()
                .filter(r -> r.origin() == NotificationOrigin.REPEAT).findFirst().orElseThrow();
        assertEquals(NotificationDecision.NOTIFIED, repeat.decision());
        assertEquals(2, repeat.fireOccurrence());
        assertEquals(1, repeat.notifyRound());
        assertNotEquals(suppressedFire.notificationId(), repeat.notificationId(),
                "被冷却抑制的重复项与冷却后的新一轮通知必须可区分");
    }

    @Test
    @DisplayName("同样的输入序列重放两遍，通知结论完全一致（可复现）")
    void sameInputSequenceReproducesConclusions() {
        double[] script = {150, 10, 150, 160, 10, 10, 170, 180};
        List<NotificationRecord> run1 = runScript(script);
        List<NotificationRecord> run2 = runScript(script);
        logRecords("REPLAY-1", run1);
        logRecords("REPLAY-2", run2);
        assertEquals(run1, run2, "相同输入序列的通知台账必须逐条一致");
        assertTrue(run1.size() >= 3, "脚本应产生足够丰富的通知记录用于比对");
    }

    private List<NotificationRecord> runScript(double[] values) {
        TestHarness h = harnessWithCooldown(50_000);
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));
        for (int i = 0; i < values.length; i++) {
            drive(h, "a", i, values[i]);
        }
        return h.notifications.records("r1");
    }
}
