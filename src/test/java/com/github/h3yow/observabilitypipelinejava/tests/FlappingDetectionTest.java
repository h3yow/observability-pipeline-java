package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import com.github.h3yow.observabilitypipelinejava.model.NotificationDecision;
import com.github.h3yow.observabilitypipelinejava.model.NotificationKind;
import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 抖动识别：观察窗口内触发/恢复转移次数达阈值 → 进入降噪（不逐条通知）；
 * 连续稳定满足/不满足达设定次数 → 退出并给出一次可区分结论。
 * 只抖一两次不得误判。每条记录的输入与判定依据可在日志中一一核对。
 */
class FlappingDetectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlappingDetectionTest.class);

    private static final Map<String, String> HOST_A = Map.of("host", "a");
    private static final Map<String, String> HOST_B = Map.of("host", "b");

    /** 窗口评估间隔约 40~80s：T0..T3 的 4 次转移落在 200s 观察窗内。 */
    private static final long FLAP_WINDOW = 200_000;
    private static final int FLAP_THRESHOLD = 4;
    private static final int EXIT_STABLE = 2;

    private int logged;

    private TestHarness flapHarness() {
        ObservabilityProperties p = TestHarness.defaultProps();
        p.setNotificationFlapWindowMs(FLAP_WINDOW);
        p.setNotificationFlapTransitionThreshold(FLAP_THRESHOLD);
        p.setNotificationFlapExitStableWindows(EXIT_STABLE);
        return TestHarness.create(p);
    }

    private void logNew(TestHarness h, String ruleId) {
        List<NotificationRecord> all = h.notifications.list(ruleId);
        for (int i = logged; i < all.size(); i++) {
            NotificationRecord r = all.get(i);
            LOG.info("NOTIFY kind={} decision={} round={} occurrence={} windowStart={} detail={}",
                    r.kind(), r.decision(), r.notificationRound(), r.fireOccurrence(),
                    r.windowStartMs(), r.detail());
        }
        logged = all.size();
    }

    private List<NotificationRecord> delivered(TestHarness h, NotificationKind kind) {
        return h.notifications.list("r1").stream()
                .filter(r -> r.kind() == kind && r.decision() == NotificationDecision.DELIVERED)
                .toList();
    }

    private boolean flapping(TestHarness h, Map<String, String> labels) {
        var snap = h.notificationGate.tracker("r1", TestHarness.fingerprint(labels));
        return snap != null && snap.flapping();
    }

    @Test
    @DisplayName("转移次数达阈值进入降噪；连续稳定不满足退出并补一次恢复结论")
    void flappingEnterAndExitOnStableUnmatched() {
        TestHarness h = flapHarness();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        h.driveWindow("latency", HOST_A, 0, 150); // T0 FIRED 下发
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 1, 10);  // T1 RECOVERED 下发
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 2, 150); // T2 FIRED 下发（第 3 次转移，未达阈值）
        logNew(h, "r1");
        assertFalse(flapping(h, HOST_A), "3 次转移未达阈值 4，不得误判");

        h.driveWindow("latency", HOST_A, 3, 10);  // T3 第 4 次转移 → 进入降噪
        logNew(h, "r1");
        assertTrue(flapping(h, HOST_A), "4 次转移达到阈值，进入抖动降噪");

        h.driveWindow("latency", HOST_A, 4, 10);  // T4 稳定不满足 1
        logNew(h, "r1");
        assertTrue(flapping(h, HOST_A), "稳定计数未达退出条件，保持降噪");
        h.driveWindow("latency", HOST_A, 5, 10);  // T5 稳定不满足 2 → 退出
        logNew(h, "r1");
        assertFalse(flapping(h, HOST_A), "连续稳定不满足达 2，退出降噪");

        assertEquals(2, delivered(h, NotificationKind.FIRED).size(),
                "降噪前两次触发正常下发，降噪期间不逐条发");
        assertEquals(2, delivered(h, NotificationKind.RECOVERED).size(),
                "T1 恢复 + 退出时补一次恢复结论");
        assertEquals(1, delivered(h, NotificationKind.FLAPPING_ENTERED).size());
        assertEquals(1, delivered(h, NotificationKind.FLAPPING_EXITED).size());

        NotificationRecord entered = delivered(h, NotificationKind.FLAPPING_ENTERED).get(0);
        LOG.info("flap-enter evidence: {}", entered.detail());
        assertTrue(entered.detail().contains("transitions=4"), "进入依据可核对");
        NotificationRecord exited = delivered(h, NotificationKind.FLAPPING_EXITED).get(0);
        assertTrue(exited.detail().contains("stableUnmatched=2"), "退出依据可核对");

        assertTrue(h.notifications.list("r1").stream()
                .anyMatch(r -> r.decision() == NotificationDecision.SUPPRESSED_FLAPPING
                        && r.kind() == NotificationKind.RECOVERED),
                "降噪期间的恢复被抑制且可区分");
    }

    @Test
    @DisplayName("降噪期间转连续稳定满足：退出并补齐 恢复旧轮次+触发新轮次 的结论")
    void flappingExitOnStableMatched() {
        TestHarness h = flapHarness();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        h.driveWindow("latency", HOST_A, 0, 150);
        h.driveWindow("latency", HOST_A, 1, 10);
        h.driveWindow("latency", HOST_A, 2, 150);
        h.driveWindow("latency", HOST_A, 3, 10);  // 进入降噪
        logNew(h, "r1");
        assertTrue(flapping(h, HOST_A));

        h.driveWindow("latency", HOST_A, 4, 150); // 稳定满足 1（FIRED 事件被降噪抑制）
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 5, 150); // 稳定满足 2 → 退出
        logNew(h, "r1");
        assertFalse(flapping(h, HOST_A));

        NotificationRecord exited = delivered(h, NotificationKind.FLAPPING_EXITED).get(0);
        assertTrue(exited.detail().contains("stableMatched=2"), "按稳定满足退出");

        // 下发的通知流必须保持 FIRED/RECOVERED 交替，与真实状态一致：
        // F(occ1) R(occ1) F(occ2) [降噪] R(occ2 结清) F(occ3 新结论)
        List<NotificationKind> stream = new ArrayList<>();
        for (NotificationRecord r : h.notifications.list("r1")) {
            if (r.decision() == NotificationDecision.DELIVERED
                    && (r.kind() == NotificationKind.FIRED || r.kind() == NotificationKind.RECOVERED)) {
                stream.add(r.kind());
            }
        }
        LOG.info("delivered stream: {}", stream);
        assertEquals(List.of(NotificationKind.FIRED, NotificationKind.RECOVERED,
                NotificationKind.FIRED, NotificationKind.RECOVERED, NotificationKind.FIRED),
                stream, "退出降噪后通知流与真实状态一致，不丢恢复、不重复触发");
    }

    @Test
    @DisplayName("只抖一两次不误判为抖动，通知照常")
    void noFalsePositiveForOccasionalFlaps() {
        TestHarness h = flapHarness();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        h.driveWindow("latency", HOST_B, 0, 150); // FIRED
        h.driveWindow("latency", HOST_B, 1, 10);  // RECOVERED（仅 2 次转移）
        h.driveWindow("latency", HOST_B, 2, 10);
        h.driveWindow("latency", HOST_B, 3, 10);
        logNew(h, "r1");

        assertFalse(flapping(h, HOST_B), "2 次转移远低于阈值，不得进入降噪");
        assertEquals(1, delivered(h, NotificationKind.FIRED).size());
        assertEquals(1, delivered(h, NotificationKind.RECOVERED).size());
        assertEquals(0, delivered(h, NotificationKind.FLAPPING_ENTERED).size());
        assertTrue(h.notifications.list("r1").stream()
                .noneMatch(r -> r.decision() == NotificationDecision.SUPPRESSED_FLAPPING));
    }
}
