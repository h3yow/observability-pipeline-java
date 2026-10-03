package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.AlertState;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import com.github.h3yow.observabilitypipelinejava.model.NotificationDecision;
import com.github.h3yow.observabilitypipelinejava.model.NotificationKind;
import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import com.github.h3yow.observabilitypipelinejava.model.SilenceOutcome;
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
 * 静默/维护窗口：只抑制对外通知，不改写状态真相；静默结束立即给出与真实状态
 * 一致的结论（不凭空恢复、不补发积压）；设置/查询/取消的各类结果可区分。
 */
class SilenceControlTest {

    private static final Logger LOG = LoggerFactory.getLogger(SilenceControlTest.class);

    private static final Map<String, String> HOST_A = Map.of("host", "a");
    private static final Map<String, String> HOST_B = Map.of("host", "b");

    private int logged;

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

    private AlertRule rule(String id) {
        return new AlertRule(id, "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1);
    }

    private SilenceOutcome silence(TestHarness h, String ruleId, Set<String> scope,
                                   long startMs, long endMs) {
        SilenceWindow w = new SilenceWindow(SilenceWindow.deriveId(ruleId, scope, startMs, endMs),
                ruleId, scope, startMs, endMs, "test", h.now());
        SilenceOutcome o = h.silences.create(w, h.now());
        LOG.info("SILENCE set rule={} scope={} [{},{}) -> {} | {}", ruleId, scope, startMs, endMs,
                o.code(), o.detail());
        return o;
    }

    private long delivered(TestHarness h, String ruleId, NotificationKind kind) {
        return h.notifications.list(ruleId).stream()
                .filter(r -> r.kind() == kind && r.decision() == NotificationDecision.DELIVERED)
                .count();
    }

    @Test
    @DisplayName("静默期间状态机照常判定但不发通知；静默结束不凭空恢复、不补发积压")
    void silenceSuppressesNotifyButKeepsTruth() {
        TestHarness h = TestHarness.create();
        h.rules.put(rule("r1"));
        // 覆盖前三窗口的评估时刻（T0≈+30s, T1≈+70s, T2≈+120s），T3≈+180s 时已结束。
        assertEquals(SilenceResultCode.CREATED,
                silence(h, "r1", Set.of(), h.now(), h.now() + 125_000).code());

        h.driveWindow("latency", HOST_A, 0, 150);
        logNew(h, "r1");
        assertEquals(AlertState.FIRING,
                h.alertStates.get(h.alertStates.keyOf("r1", TestHarness.fingerprint(HOST_A))).state(),
                "静默不改写状态真相：仍应进入 FIRING");
        assertTrue(h.alertStates.events("r1").stream()
                .anyMatch(e -> e.type() == AlertEventType.FIRED), "生命周期事件照常产生");

        h.driveWindow("latency", HOST_A, 1, 150);
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 2, 10);
        logNew(h, "r1");
        assertTrue(h.alertStates.events("r1").stream()
                .anyMatch(e -> e.type() == AlertEventType.RECOVERED), "恢复事件照常产生");

        h.driveWindow("latency", HOST_A, 3, 10); // 静默已结束
        logNew(h, "r1");

        List<NotificationRecord> all = h.notifications.list("r1");
        assertTrue(all.stream().allMatch(r -> r.decision() != NotificationDecision.DELIVERED),
                "全程无下发：触发与恢复都被静默，结束后也不补发");
        assertTrue(all.stream().anyMatch(r -> r.decision() == NotificationDecision.SUPPRESSED_SILENCE
                && r.kind() == NotificationKind.FIRED));
        assertTrue(all.stream().anyMatch(r -> r.decision() == NotificationDecision.SUPPRESSED_SILENCE
                && r.kind() == NotificationKind.RECOVERED));
        assertEquals(0, delivered(h, "r1", NotificationKind.RECOVERED),
                "触发未通知过，静默结束后不得凭空冒出恢复");
    }

    @Test
    @DisplayName("静默结束仍触发：补发一次可区分结论，随后恢复照常，不形成通知串")
    void silenceExitWhileStillFiringYieldsSingleConclusion() {
        TestHarness h = TestHarness.create();
        h.rules.put(rule("r1"));
        // 覆盖前两窗口评估（T0≈+30s, T1≈+70s），T2≈+120s 时已结束。
        silence(h, "r1", Set.of(), h.now(), h.now() + 75_000);

        h.driveWindow("latency", HOST_A, 0, 150); // FIRED 被静默
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 1, 150); // 仍 FIRING，静默中
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 2, 150); // 静默结束，仍 FIRING → 补发一次结论
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 3, 150); // 无变化，不得重复补发
        logNew(h, "r1");
        h.driveWindow("latency", HOST_A, 4, 10);  // 恢复，照常下发
        logNew(h, "r1");

        List<NotificationRecord> firedDelivered = h.notifications.list("r1").stream()
                .filter(r -> r.kind() == NotificationKind.FIRED
                        && r.decision() == NotificationDecision.DELIVERED).toList();
        assertEquals(1, firedDelivered.size(), "静默结束后只补发一次触发结论，不补发积压");
        assertEquals(1, firedDelivered.get(0).notificationRound());
        assertEquals(null, firedDelivered.get(0).sourceEventId(),
                "补发结论无源事件（由对账产生），与窗口事件触发的通知可区分");
        LOG.info("silence-exit conclusion: {}", firedDelivered.get(0));
        assertEquals(1, delivered(h, "r1", NotificationKind.RECOVERED), "恢复照常下发一次");
    }

    @Test
    @DisplayName("维度级静默：只静默指定维度，其余维度通知不受影响")
    void dimensionScopedSilence() {
        TestHarness h = TestHarness.create();
        h.rules.put(rule("r1"));
        String fpA = TestHarness.fingerprint(HOST_A);
        silence(h, "r1", Set.of(fpA), h.now(), h.now() + 75_000);

        // 窗口0：a、b 同时触发
        long w0 = h.timing.windowStartFor(h.now());
        h.send("latency", HOST_A, w0 + 1_000, 150, "probe");
        h.send("latency", HOST_B, w0 + 1_000, 160, "probe");
        h.advanceMs(w0 + 30_001 - h.now());
        h.tick();
        logNew(h, "r1");

        List<NotificationRecord> all = h.notifications.list("r1");
        assertTrue(all.stream().anyMatch(r -> r.fingerprint().equals(fpA)
                && r.decision() == NotificationDecision.SUPPRESSED_SILENCE), "a 被静默");
        String fpB = TestHarness.fingerprint(HOST_B);
        assertTrue(all.stream().anyMatch(r -> r.fingerprint().equals(fpB)
                && r.kind() == NotificationKind.FIRED
                && r.decision() == NotificationDecision.DELIVERED), "b 不在静默范围，照常下发");
    }

    @Test
    @DisplayName("静默设置/取消的各类结果可区分：重复、过去、非法区间、重叠、容量、取消")
    void silenceCrudOutcomes() {
        var props = TestHarness.defaultProps();
        props.setNotificationMaxSilences(3);
        TestHarness h = TestHarness.create(props);
        long now = h.now();

        SilenceOutcome created = silence(h, "r1", Set.of(), now, now + 60_000);
        assertEquals(SilenceResultCode.CREATED, created.code());

        SilenceOutcome dup = silence(h, "r1", Set.of(), now, now + 60_000);
        assertEquals(SilenceResultCode.REJECTED_DUPLICATE, dup.code(), "相同内容重复设置");

        SilenceOutcome past = silence(h, "r1", Set.of(), now - 10_000, now - 1);
        assertEquals(SilenceResultCode.REJECTED_PAST, past.code(), "整体在过去的区间");

        SilenceOutcome invalid = silence(h, "r1", Set.of(), now + 5_000, now + 5_000);
        assertEquals(SilenceResultCode.REJECTED_INVALID_RANGE, invalid.code(), "end<=start");

        SilenceOutcome overlap = silence(h, "r1", Set.of("fp-x"), now + 30_000, now + 90_000);
        assertEquals(SilenceResultCode.REJECTED_OVERLAP, overlap.code(),
                "维度级与全规则静默时间重叠也算重叠");

        SilenceOutcome disjointScope = silence(h, "r2", Set.of("fp-x"), now + 30_000, now + 90_000);
        assertEquals(SilenceResultCode.CREATED, disjointScope.code(), "不同规则不冲突");

        SilenceOutcome capacity = silence(h, "r3", Set.of(), now, now + 60_000);
        assertEquals(SilenceResultCode.CREATED, capacity.code());
        SilenceOutcome overCapacity = silence(h, "r4", Set.of(), now, now + 60_000);
        assertEquals(SilenceResultCode.REJECTED_CAPACITY, overCapacity.code(),
                "达到容量上限要给出可区分结果");

        SilenceOutcome cancel = h.silences.cancel(created.silence().id());
        LOG.info("SILENCE cancel {} -> {} | {}", created.silence().id(), cancel.code(), cancel.detail());
        assertEquals(SilenceResultCode.CANCELLED, cancel.code());
        SilenceOutcome cancelAgain = h.silences.cancel(created.silence().id());
        assertEquals(SilenceResultCode.NOT_FOUND, cancelAgain.code(), "重复取消可区分");

        // 取消后容量释放；过期静默在写入时被回收
        SilenceOutcome afterCancel = silence(h, "r4", Set.of(), now, now + 60_000);
        assertEquals(SilenceResultCode.CREATED, afterCancel.code());
        h.advanceMs(200_000); // 全部静默过期
        SilenceOutcome afterExpiry = silence(h, "r5", Set.of(), h.now(), h.now() + 60_000);
        assertEquals(SilenceResultCode.CREATED, afterExpiry.code(), "过期静默被回收，容量释放");
        assertFalse(h.silences.list(null).stream()
                .anyMatch(w -> w.endMs() <= h.now()), "过期静默不应残留");
    }
}
