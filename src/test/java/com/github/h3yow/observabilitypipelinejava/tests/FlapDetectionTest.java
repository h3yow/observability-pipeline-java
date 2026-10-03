package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import com.github.h3yow.observabilitypipelinejava.model.FlapSnapshot;
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
 * 抖动识别：观察窗口内触发/恢复切换达到阈值进入降噪，期间通知被抑制；
 * 连续稳定（满足或不满足）达到设定次数后退出并给出一条可区分结论；
 * 只抖一两次不误判。每条判定的输入（窗口、匹配结果、切换计数）都打在日志里。
 */
class FlapDetectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlapDetectionTest.class);

    private static final long W = 10_000;
    private static final long GRACE = 20_000;

    private TestHarness harness() {
        ObservabilityProperties props = TestHarness.defaultProps();
        props.setNotificationCooldownMs(0);      // 关闭冷却，隔离抖动变量
        props.setFlapWindowMs(10_000_000);       // 观察窗口覆盖整个测试过程
        props.setFlapTransitionThreshold(4);     // 4 次切换才判定抖动
        props.setFlapStableWindows(2);           // 连续稳定 2 个窗口退出
        return TestHarness.create(props);
    }

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
        for (FlapSnapshot s : h.flapTracker.snapshots()) {
            LOG.info("FLAP-INPUT window={} matched={} -> damping={} transitions={} "
                            + "stableMatched={} stableUnmatched={}",
                    targetWindow, value > 100.0, s.damping(), s.transitionsInWindow(),
                    s.stableMatchedWindows(), s.stableUnmatchedWindows());
        }
    }

    private void logRecords(String tag, List<NotificationRecord> records) {
        for (NotificationRecord r : records) {
            LOG.info("{} kind={} origin={} decision={} occurrence={} round={} window={}",
                    tag, r.kind(), r.origin(), r.decision(), r.fireOccurrence(),
                    r.notifyRound(), r.windowStartMs());
        }
    }

    @Test
    @DisplayName("触发/恢复来回切换 4 次进入降噪；连续稳定满足 2 窗退出并给出 FLAP_EXIT 结论")
    void oscillationEntersDampingAndExitsOnStableMatch() {
        TestHarness h = harness();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        drive(h, "a", 0, 150); // FIRED   (切换 1) 正常通知
        drive(h, "a", 1, 10);  // RECOVERED(切换 2) 正常通知
        drive(h, "a", 2, 150); // FIRED   (切换 3) 正常通知
        drive(h, "a", 3, 10);  // RECOVERED(切换 4) → 进入降噪，本条被抑制

        FlapSnapshot damping = h.flapTracker.snapshots().get(0);
        LOG.info("FLAP-STATE after 4 transitions: damping={} transitions={}",
                damping.damping(), damping.transitionsInWindow());
        assertTrue(damping.damping(), "4 次切换应进入降噪");
        assertEquals(4, damping.transitionsInWindow());

        drive(h, "a", 4, 150); // 降噪中：FIRED 被抑制；稳定满足 1
        drive(h, "a", 5, 150); // 稳定满足 2 → 退出降噪，给出 FLAP_EXIT 结论

        FlapSnapshot exited = h.flapTracker.snapshots().get(0);
        assertFalse(exited.damping(), "连续稳定满足 2 窗应退出降噪");

        List<NotificationRecord> records = h.notifications.records("r1");
        logRecords("FLAP-EXIT-MATCH", records);
        assertEquals(6, records.size());

        // 进入降噪前的三条正常通知。
        assertEquals(NotificationDecision.NOTIFIED, records.get(0).decision());
        assertEquals(NotificationDecision.NOTIFIED, records.get(1).decision());
        assertEquals(NotificationDecision.NOTIFIED, records.get(2).decision());
        // 第 4 次切换（RECOVERED）与降噪中的 FIRED 都被抑制，且原因可区分。
        assertEquals(AlertEventType.RECOVERED, records.get(3).kind());
        assertEquals(NotificationDecision.SUPPRESSED_BY_FLAPPING, records.get(3).decision());
        assertEquals(AlertEventType.FIRED, records.get(4).kind());
        assertEquals(NotificationDecision.SUPPRESSED_BY_FLAPPING, records.get(4).decision());
        // 退出结论：一条可区分的 FLAP_EXIT，方向为稳定满足（FIRED）。
        NotificationRecord exit = records.get(5);
        assertEquals(NotificationOrigin.FLAP_EXIT, exit.origin());
        assertEquals(AlertEventType.FIRED, exit.kind());
        assertEquals(NotificationDecision.NOTIFIED, exit.decision());
    }

    @Test
    @DisplayName("降噪后连续稳定不满足 2 窗退出，结论方向为 RECOVERED")
    void exitsOnStableUnmatched() {
        TestHarness h = harness();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        drive(h, "a", 0, 150); // FIRED     (切换 1)
        drive(h, "a", 1, 10);  // RECOVERED (切换 2)
        drive(h, "a", 2, 150); // FIRED     (切换 3)
        drive(h, "a", 3, 10);  // RECOVERED (切换 4) → 降噪，稳定不满足 1
        drive(h, "a", 4, 10);  // 稳定不满足 2 → 退出，结论 RECOVERED

        List<NotificationRecord> records = h.notifications.records("r1");
        logRecords("FLAP-EXIT-UNMATCH", records);
        NotificationRecord exit = records.get(records.size() - 1);
        assertEquals(NotificationOrigin.FLAP_EXIT, exit.origin());
        assertEquals(AlertEventType.RECOVERED, exit.kind());
        assertEquals(NotificationDecision.NOTIFIED, exit.decision());
        assertFalse(h.flapTracker.snapshots().get(0).damping());
    }

    @Test
    @DisplayName("只抖一两次（3 次切换）不进入降噪，通知照常")
    void normalFlappingBelowThresholdNotMisjudged() {
        TestHarness h = harness();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        drive(h, "a", 0, 150); // FIRED     (切换 1)
        drive(h, "a", 1, 10);  // RECOVERED (切换 2)
        drive(h, "a", 2, 150); // FIRED     (切换 3) —— 未达阈值 4
        drive(h, "a", 3, 160); // 持续满足，无新事件

        FlapSnapshot snap = h.flapTracker.snapshots().get(0);
        LOG.info("FLAP-STATE below threshold: damping={} transitions={}",
                snap.damping(), snap.transitionsInWindow());
        assertFalse(snap.damping(), "3 次切换不应误判为抖动");
        assertEquals(3, snap.transitionsInWindow());

        List<NotificationRecord> records = h.notifications.records("r1");
        logRecords("FLAP-BELOW-THRESHOLD", records);
        assertTrue(records.stream().allMatch(r -> r.decision() == NotificationDecision.NOTIFIED),
                "未进入降噪时不应出现被抑制的记录");
        assertTrue(records.stream().noneMatch(r -> r.origin() == NotificationOrigin.FLAP_EXIT));
    }
}
