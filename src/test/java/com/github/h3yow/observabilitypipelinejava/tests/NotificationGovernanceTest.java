package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import com.github.h3yow.observabilitypipelinejava.model.SilenceResultCode;
import com.github.h3yow.observabilitypipelinejava.model.SilenceWindow;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通知控制层自身的治理：规则删除/维度老化后跟踪状态被回收，不随上报无限增长；
 * 同样的输入序列重放，通知判定结论完全一致（确定且可复现）。
 */
class NotificationGovernanceTest {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationGovernanceTest.class);

    private static final Map<String, String> HOST_A = Map.of("host", "a");

    private AlertRule rule(String id) {
        return new AlertRule(id, "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1);
    }

    @Test
    @DisplayName("规则删除后其通知跟踪器与静默窗口被回收干净")
    void ruleDeletePurgesNotificationState() {
        TestHarness h = TestHarness.create();
        h.rules.put(rule("r1"));
        SilenceWindow sw = new SilenceWindow(
                SilenceWindow.deriveId("r1", Set.of(), h.now(), h.now() + 300_000),
                "r1", Set.of(), h.now(), h.now() + 300_000, "maint", h.now());
        assertEquals(SilenceResultCode.CREATED, h.silences.create(sw, h.now()).code());

        h.driveWindow("latency", HOST_A, 0, 150); // 产生跟踪器
        assertEquals(1, h.notificationGate.trackerCount());
        assertEquals(1, h.silences.size());

        // 与 HTTP 删除规则相同的回收路径
        h.rules.remove("r1");
        int purgedTrackers = h.notificationGate.purgeRule("r1");
        int purgedSilences = h.silences.purgeRule("r1");
        LOG.info("PURGE rule=r1 -> trackers={}, silences={}", purgedTrackers, purgedSilences);
        assertEquals(1, purgedTrackers);
        assertEquals(1, purgedSilences);
        assertEquals(0, h.notificationGate.trackerCount());
        assertEquals(0, h.silences.size());
    }

    @Test
    @DisplayName("维度老化（长期无评估）后跟踪器被回收，不随时间无限增长")
    void idleTrackerEvicted() {
        ObservabilityProperties p = TestHarness.defaultProps();
        p.setNotificationTrackerIdleMs(60_000);
        TestHarness h = TestHarness.create(p);
        h.rules.put(rule("r1"));

        h.driveWindow("latency", HOST_A, 0, 150);
        assertEquals(1, h.notificationGate.trackerCount());

        h.advanceMs(200_000); // 远超 idleMs，期间该维度无新评估
        h.tick();             // 后台推进触发回收
        LOG.info("after idle: trackers={}", h.notificationGate.trackerCount());
        assertEquals(0, h.notificationGate.trackerCount(), "空闲跟踪器应被回收");
    }

    @Test
    @DisplayName("同样的输入序列重放两次，通知判定记录完全一致（确定可复现）")
    void sameInputsYieldIdenticalNotifications() {
        List<NotificationRecord> first = runScenario();
        List<NotificationRecord> second = runScenario();
        assertEquals(first.size(), second.size());
        for (int i = 0; i < first.size(); i++) {
            NotificationRecord a = first.get(i);
            NotificationRecord b = second.get(i);
            LOG.info("REPLAY[{}] {} == {} -> {}", i, a.notificationId(), b.notificationId(),
                    a.equals(b));
            assertEquals(a, b, "第 " + i + " 条通知记录两次运行必须一致");
        }
    }

    private List<NotificationRecord> runScenario() {
        ObservabilityProperties p = TestHarness.defaultProps();
        p.setNotificationCooldownMs(100_000);
        p.setNotificationFlapWindowMs(200_000);
        p.setNotificationFlapTransitionThreshold(4);
        p.setNotificationFlapExitStableWindows(2);
        TestHarness h = TestHarness.create(p);
        h.rules.put(rule("r1"));
        SilenceWindow sw = new SilenceWindow(
                SilenceWindow.deriveId("r1", Set.of(), h.now(), h.now() + 75_000),
                "r1", Set.of(), h.now(), h.now() + 75_000, "maint", h.now());
        assertTrue(h.silences.create(sw, h.now()).ok());

        // 混合序列：静默期触发 -> 静默结束补结论 -> 恢复 -> 再触发（冷却）-> 抖动
        double[] values = {150, 150, 150, 10, 150, 10, 150, 10, 150, 10, 10, 10};
        for (int i = 0; i < values.length; i++) {
            h.driveWindow("latency", HOST_A, i, values[i]);
        }
        List<NotificationRecord> records = h.notifications.list("r1");
        for (NotificationRecord r : records) {
            LOG.info("SCENARIO kind={} decision={} round={} occurrence={} windowStart={} detail={}",
                    r.kind(), r.decision(), r.notificationRound(), r.fireOccurrence(),
                    r.windowStartMs(), r.detail());
        }
        assertTrue(records.size() > 5, "场景应产生丰富的判定记录");
        return records;
    }
}
