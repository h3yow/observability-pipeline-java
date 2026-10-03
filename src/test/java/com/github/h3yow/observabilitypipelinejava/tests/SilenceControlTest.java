package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.AlertState;
import com.github.h3yow.observabilitypipelinejava.model.AlertStateSnapshot;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import com.github.h3yow.observabilitypipelinejava.model.NotificationDecision;
import com.github.h3yow.observabilitypipelinejava.model.NotificationOrigin;
import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import com.github.h3yow.observabilitypipelinejava.model.SilenceCreateStatus;
import com.github.h3yow.observabilitypipelinejava.notify.SilenceStore;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 静默/维护窗口：状态机照常推进，通知类事件被抑制且可区分；
 * 静默结束不补发积压命中、不凭空产生恢复；设置/查询/取消的结果明确可区分。
 */
class SilenceControlTest {

    private static final Logger LOG = LoggerFactory.getLogger(SilenceControlTest.class);

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

    /** 与 AlertStateTransitionTest 相同的窗口驱动：上报落在当前开放窗口，再推进到可封存。 */
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

    private void logRecords(String tag, List<NotificationRecord> records) {
        for (NotificationRecord r : records) {
            LOG.info("{} kind={} origin={} decision={} occurrence={} round={} window={}",
                    tag, r.kind(), r.origin(), r.decision(), r.fireOccurrence(),
                    r.notifyRound(), r.windowStartMs());
        }
    }

    @Test
    @DisplayName("静默期间状态照常推进但通知被抑制；静默结束仍在触发只补一条 REPEAT，不刷屏")
    void silenceSuppressesButStateAdvances() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        long w0 = h.timing.windowStartFor(h.now());
        // 驱动器每次跨约 3~4 个窗口（见 drive）：窗口下标 0/1/2 实际落在
        // w0、w0+40s、w0+80s。静默区间 [w0, w0+100s) 恰好覆盖这三次评估。
        var created = h.silences.create("r1", Map.of(), w0, w0 + 100_000, "nightly deploy",
                h.now());
        LOG.info("SILENCE create [{} , {}) -> {}", w0, w0 + 100_000, created.status());
        assertEquals(SilenceCreateStatus.CREATED, created.status());

        drive(h, "a", 0, 150); // FIRED 命中静默
        assertEquals(AlertState.FIRING, state(h, "r1", fp("a")).state(),
                "静默不改写状态机：内部应如实进入 FIRING");
        drive(h, "a", 1, 160);
        drive(h, "a", 2, 170);
        assertEquals(AlertState.FIRING, state(h, "r1", fp("a")).state());

        List<NotificationRecord> duringSilence = h.notifications.records("r1");
        logRecords("DURING", duringSilence);
        assertTrue(duringSilence.stream().allMatch(
                        r -> r.decision() == NotificationDecision.SUPPRESSED_BY_SILENCE),
                "静默期间只允许出现被静默抑制的记录");
        assertEquals(1, duringSilence.size(), "FIRING 期间重复命中不逐条产生记录");

        drive(h, "a", 3, 180); // 静默结束，仍在触发
        List<NotificationRecord> after = h.notifications.records("r1");
        logRecords("AFTER", after);
        List<NotificationRecord> notified = after.stream()
                .filter(r -> r.decision() == NotificationDecision.NOTIFIED).toList();
        assertEquals(1, notified.size(), "静默结束后仍在触发：只补一条通知，不补发积压命中");
        assertEquals(NotificationOrigin.REPEAT, notified.get(0).origin());
        assertEquals(0, notified.get(0).notifyRound());

        drive(h, "a", 4, 10); // 恢复：正常通知
        List<NotificationRecord> finalRecords = h.notifications.records("r1");
        logRecords("FINAL", finalRecords);
        assertTrue(finalRecords.stream().anyMatch(r -> r.kind().name().equals("RECOVERED")
                && r.decision() == NotificationDecision.NOTIFIED));
    }

    @Test
    @DisplayName("静默期间发生的恢复被抑制；静默结束后不凭空冒出第二条恢复")
    void recoveryDuringSilenceIsNotReplayed() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        long w0 = h.timing.windowStartFor(h.now());
        // 窗口下标 1/2 实际落在 w0+40s、w0+80s：静默 [w0+40s, w0+90s) 覆盖恢复发生的窗口。
        var created = h.silences.create("r1", Map.of(), w0 + 40_000, w0 + 90_000, "maint",
                h.now());
        LOG.info("SILENCE create [{}, {}) -> {}", w0 + 40_000, w0 + 90_000, created.status());
        assertEquals(SilenceCreateStatus.CREATED, created.status());

        drive(h, "a", 0, 150); // 先真实触发并通知（不在静默内）
        assertEquals(AlertState.FIRING, state(h, "r1", fp("a")).state());

        drive(h, "a", 1, 10); // RECOVERED 被静默
        assertEquals(AlertState.RECOVERED, state(h, "r1", fp("a")).state());
        drive(h, "a", 2, 10); // 回 INACTIVE
        drive(h, "a", 3, 10); // 静默已结束，无新事件

        List<NotificationRecord> records = h.notifications.records("r1");
        logRecords("RECOVER-IN-SILENCE", records);
        List<NotificationRecord> recoveries = records.stream()
                .filter(r -> r.kind() == com.github.h3yow.observabilitypipelinejava
                        .model.AlertEventType.RECOVERED).toList();
        assertEquals(1, recoveries.size(), "恢复只被判定一次，静默结束后不重复冒出");
        assertEquals(NotificationDecision.SUPPRESSED_BY_SILENCE, recoveries.get(0).decision());
        assertEquals(AlertState.INACTIVE, state(h, "r1", fp("a")).state(),
                "状态如实反映：已回到 INACTIVE");
    }

    @Test
    @DisplayName("维度级静默：只抑制选择器匹配的维度，其余维度照常通知")
    void dimensionScopedSilence() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        long w0 = h.timing.windowStartFor(h.now());
        var created = h.silences.create("r1", Map.of("host", "a"), w0, w0 + W, "only host a",
                h.now());
        assertEquals(SilenceCreateStatus.CREATED, created.status());

        // 同一窗口两个维度都命中。
        h.send("latency", Map.of("host", "a"), w0 + 1_000, 150, "probe");
        h.send("latency", Map.of("host", "b"), w0 + 1_000, 160, "probe");
        h.advanceMs(W + GRACE + 1);
        h.tick();

        List<NotificationRecord> records = h.notifications.records("r1");
        logRecords("DIM-SCOPED", records);
        NotificationRecord forA = records.stream()
                .filter(r -> r.fingerprint().equals(fp("a"))).findFirst().orElseThrow();
        NotificationRecord forB = records.stream()
                .filter(r -> r.fingerprint().equals(fp("b"))).findFirst().orElseThrow();
        assertEquals(NotificationDecision.SUPPRESSED_BY_SILENCE, forA.decision());
        assertEquals(NotificationDecision.NOTIFIED, forB.decision());
        // 两者状态都如实为 FIRING。
        assertEquals(AlertState.FIRING, state(h, "r1", fp("a")).state());
        assertEquals(AlertState.FIRING, state(h, "r1", fp("b")).state());
    }

    @Test
    @DisplayName("静默设置结果可区分：非法区间/落在过去/重复/重叠/上限/取消")
    void silenceCrudResultsAreDistinct() {
        ObservabilityProperties props = TestHarness.defaultProps();
        props.setMaxSilences(2);
        TestHarness h = TestHarness.create(props);
        long now = h.now();

        var invalid = h.silences.create("r1", Map.of(), now + 10_000, now + 10_000, "x", now);
        LOG.info("CREATE end==start -> {}", invalid.status());
        assertEquals(SilenceCreateStatus.INVALID_RANGE, invalid.status());

        var past = h.silences.create("r1", Map.of(), now - 20_000, now - 10_000, "x", now);
        LOG.info("CREATE entirely in past -> {}", past.status());
        assertEquals(SilenceCreateStatus.IN_PAST, past.status());

        var ok = h.silences.create("r1", Map.of(), now + 10_000, now + 20_000, "x", now);
        assertEquals(SilenceCreateStatus.CREATED, ok.status());

        var dup = h.silences.create("r1", Map.of(), now + 10_000, now + 20_000, "x", now);
        LOG.info("CREATE identical -> {}", dup.status());
        assertEquals(SilenceCreateStatus.DUPLICATE, dup.status());

        var overlap = h.silences.create("r1", Map.of(), now + 15_000, now + 25_000, "x", now);
        LOG.info("CREATE overlapping -> {}", overlap.status());
        assertEquals(SilenceCreateStatus.OVERLAP, overlap.status());

        // 不同选择器不算重叠。
        var otherDim = h.silences.create("r1", Map.of("host", "a"), now + 15_000, now + 25_000,
                "x", now);
        assertEquals(SilenceCreateStatus.CREATED, otherDim.status());

        // 上限=maxSilences=2：再新增（不同规则）达到上限。
        var limited = h.silences.create("r2", Map.of(), now + 10_000, now + 20_000, "x", now);
        LOG.info("CREATE beyond limit -> {}", limited.status());
        assertEquals(SilenceCreateStatus.LIMIT_REACHED, limited.status());

        assertEquals(2, h.silences.list(null).size());
        assertEquals(1, h.silences.list("r2").size() + h.silences.list("r1").size() - 1,
                "按规则过滤查询");

        assertTrue(h.silences.cancel(ok.silence().id()), "首次取消成功");
        assertFalse(h.silences.cancel(ok.silence().id()), "重复取消返回 false，可区分");
    }

    @Test
    @DisplayName("过期静默随窗口推进回收，不再影响后续判定")
    void expiredSilencesArePrunedOnTick() {
        TestHarness h = TestHarness.create();
        h.rules.put(new AlertRule("r1", "latency", Map.of(), AggregateFunction.AVG,
                Comparator.GT, 100.0, 1));

        long w0 = h.timing.windowStartFor(h.now());
        var created = h.silences.create("r1", Map.of(), w0, w0 + W, "short", h.now());
        assertEquals(SilenceCreateStatus.CREATED, created.status());
        assertEquals(1, h.silences.size());

        drive(h, "a", 0, 150); // 命中静默
        drive(h, "a", 1, 160); // 静默已过期（end=w0+W <= 评估时刻），被回收
        LOG.info("silences after ticks: size={}", h.silences.size());
        assertEquals(0, h.silences.size(), "过期静默应被回收");

        List<NotificationRecord> records = h.notifications.records("r1");
        logRecords("EXPIRE", records);
        assertTrue(records.stream().anyMatch(r -> r.decision() == NotificationDecision.NOTIFIED),
                "静默过期后仍在触发应能补一条通知");
    }
}
