package com.github.h3yow.observabilitypipelinejava.notify;

import com.github.h3yow.observabilitypipelinejava.model.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.model.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.model.AlertRule;
import com.github.h3yow.observabilitypipelinejava.model.AlertState;
import com.github.h3yow.observabilitypipelinejava.model.AlertStateSnapshot;
import com.github.h3yow.observabilitypipelinejava.model.FlapSnapshot;
import com.github.h3yow.observabilitypipelinejava.model.NotificationDecision;
import com.github.h3yow.observabilitypipelinejava.model.NotificationOrigin;
import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import com.github.h3yow.observabilitypipelinejava.model.WindowAggregate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 通知控制层：叠加在告警状态机之上，只决定"对外通知"，不改写状态结论。
 *
 * <ul>
 *   <li>静默：命中静默区间的通知类事件被抑制并记录；状态机照常推进，
 *       静默结束后不补发积压命中、也不凭空产生恢复；</li>
 *   <li>冷却：FIRED 通知后进入冷却期，期内重复命中被抑制；
 *       冷却结束仍在 FIRING 则补发一条 REPEAT（轮次递增，可区分）；
 *       RECOVERED 不受冷却影响；</li>
 *   <li>抖动：观察窗口内切换过频进入降噪，期内通知被抑制；
 *       连续稳定达到设定次数后退出并给出一条 FLAP_EXIT 结论。</li>
 * </ul>
 * 全部判定以窗口起点为时间基准，同样的输入顺序结论可复现。
 * 冷却/抖动跟踪条目有上限，随窗口推进与维度老化回收，规则删除时即刻回收。
 */
public class NotificationController {

    /** 冷却跟踪条目：某维度最近一次 FIRED 通知落在哪个窗口、第几轮。 */
    private static final class CooldownEntry {
        long lastNotifiedWindowMs;
        int round;
        int occurrence;
        /** 维度最近一次被评估的窗口起点：老化回收以此为准，避免误伤冷却期内的条目。 */
        long lastActivityWindowMs;

        CooldownEntry(long windowMs, int round, int occurrence) {
            this.lastNotifiedWindowMs = windowMs;
            this.round = round;
            this.occurrence = occurrence;
            this.lastActivityWindowMs = windowMs;
        }
    }

    private final SilenceStore silenceStore;
    private final FlapTracker flapTracker;
    private final long cooldownMs;
    private final int maxTrackers;
    private final Map<String, CooldownEntry> cooldowns = new HashMap<>();
    private final CopyOnWriteArrayList<NotificationRecord> log = new CopyOnWriteArrayList<>();
    private long cooldownEvictedCount;

    public NotificationController(SilenceStore silenceStore, FlapTracker flapTracker,
                                  long cooldownMs, int maxTrackers) {
        if (cooldownMs < 0 || maxTrackers < 1) {
            throw new IllegalArgumentException("invalid notification controller config");
        }
        this.silenceStore = silenceStore;
        this.flapTracker = flapTracker;
        this.cooldownMs = cooldownMs;
        this.maxTrackers = maxTrackers;
    }

    /**
     * 一个 (规则, 维度) 在一个已封存窗口评估完成后的通知判定。
     * 只能由 AlertEvaluator 在评估锁内按窗口顺序调用。
     *
     * @param matched   本窗口条件是否满足
     * @param rawEvents 状态机本窗口为该维度产生的生命周期事件（可能为空）
     * @return 本次产生的通知台账记录（发出与抑制都记录），顺序确定
     */
    public List<NotificationRecord> onDimensionEvaluated(AlertRule rule, WindowAggregate agg,
                                                         AlertStateSnapshot current,
                                                         boolean matched,
                                                         List<AlertEvent> rawEvents,
                                                         long evaluatedAtMs) {
        String key = keyOf(rule.id(), agg.getFingerprint());
        long windowStart = agg.getWindowStartMs();
        boolean silenced = silenceStore.isSilenced(rule.id(), agg.getLabels(), windowStart);

        // 维度仍被评估即视为活跃：刷新冷却条目的活动时间，老化回收以此为准。
        CooldownEntry active = cooldowns.get(key);
        if (active != null) {
            active.lastActivityWindowMs = Math.max(active.lastActivityWindowMs, windowStart);
        }

        // 1. 抖动跟踪：先登记本窗口的状态切换，再用本窗口匹配结果推进判定。
        for (AlertEvent e : rawEvents) {
            if (e.type() == AlertEventType.FIRED || e.type() == AlertEventType.RECOVERED) {
                flapTracker.recordTransition(key, rule.id(), agg.getFingerprint(), windowStart);
            }
        }
        FlapTracker.FlapAssessment flap = flapTracker.assess(key, rule.id(),
                agg.getFingerprint(), matched, windowStart);

        List<NotificationRecord> out = new ArrayList<>();
        boolean firedNotifiedThisWindow = false;

        // 2. 抖动退出：给出一条可区分的一次性结论（kind 取稳定方向）。
        if (flap.justExited()) {
            AlertEventType kind = flap.exitedStableMatched()
                    ? AlertEventType.FIRED : AlertEventType.RECOVERED;
            NotificationDecision decision = silenced
                    ? NotificationDecision.SUPPRESSED_BY_SILENCE : NotificationDecision.NOTIFIED;
            int round = 0;
            if (decision == NotificationDecision.NOTIFIED && kind == AlertEventType.FIRED) {
                markNotified(key, windowStart, 0, current.fireOccurrence());
                firedNotifiedThisWindow = true;
            }
            out.add(record(rule, agg, kind, NotificationOrigin.FLAP_EXIT, decision,
                    current.fireOccurrence(), round, current.lastObservedValue(),
                    evaluatedAtMs, null));
        }

        // 3. 状态机产生的通知类事件：静默 → 抖动 → 冷却 依次判定。
        for (AlertEvent e : rawEvents) {
            if (e.type() != AlertEventType.FIRED && e.type() != AlertEventType.RECOVERED) {
                continue; // PENDING_STARTED 不是通知类事件
            }
            NotificationDecision decision;
            if (silenced) {
                decision = NotificationDecision.SUPPRESSED_BY_SILENCE;
            } else if (flap.damping()) {
                decision = NotificationDecision.SUPPRESSED_BY_FLAPPING;
            } else if (e.type() == AlertEventType.FIRED && inCooldown(key, windowStart)) {
                decision = NotificationDecision.SUPPRESSED_BY_COOLDOWN;
            } else {
                decision = NotificationDecision.NOTIFIED;
            }
            if (decision == NotificationDecision.NOTIFIED && e.type() == AlertEventType.FIRED) {
                markNotified(key, windowStart, 0, e.fireOccurrence());
                firedNotifiedThisWindow = true;
            }
            out.add(record(rule, agg, e.type(), NotificationOrigin.INITIAL, decision,
                    e.fireOccurrence(), 0, e.observedValue(), evaluatedAtMs, e.eventId()));
        }

        // 4. 冷却后再通知：仍在 FIRING 且本窗口没有发出 FIRED 类通知时，
        //    冷却期满（或从未通知过，例如首次 FIRED 被静默）补发一条 REPEAT。
        if (current.state() == AlertState.FIRING && !firedNotifiedThisWindow
                && !silenced && !flap.damping()) {
            CooldownEntry ce = cooldowns.get(key);
            boolean hasRawFired = rawEvents.stream().anyMatch(e -> e.type() == AlertEventType.FIRED);
            if (!hasRawFired && (ce == null
                    || windowStart - ce.lastNotifiedWindowMs >= cooldownMs)) {
                int round = ce == null ? 0 : ce.round + 1;
                markNotified(key, windowStart, round, current.fireOccurrence());
                out.add(record(rule, agg, AlertEventType.FIRED, NotificationOrigin.REPEAT,
                        NotificationDecision.NOTIFIED, current.fireOccurrence(), round,
                        current.lastObservedValue(), evaluatedAtMs, null));
            }
        }

        log.addAll(out);
        return out;
    }

    private boolean inCooldown(String key, long windowStart) {
        CooldownEntry ce = cooldowns.get(key);
        return ce != null && windowStart - ce.lastNotifiedWindowMs < cooldownMs;
    }

    private void markNotified(String key, long windowStart, int round, int occurrence) {
        CooldownEntry ce = cooldowns.get(key);
        if (ce == null) {
            if (cooldowns.size() >= maxTrackers) {
                evictOldestCooldown();
            }
            cooldowns.put(key, new CooldownEntry(windowStart, round, occurrence));
        } else {
            ce.lastNotifiedWindowMs = windowStart;
            ce.round = round;
            ce.occurrence = occurrence;
        }
    }

    /** 淘汰通知窗口最旧的冷却条目；键序兜底保证确定。 */
    private void evictOldestCooldown() {
        String victim = null;
        long oldest = Long.MAX_VALUE;
        List<String> keys = new ArrayList<>(cooldowns.keySet());
        keys.sort(String::compareTo);
        for (String k : keys) {
            long t = cooldowns.get(k).lastNotifiedWindowMs;
            if (t < oldest) {
                oldest = t;
                victim = k;
            }
        }
        if (victim != null) {
            cooldowns.remove(victim);
            cooldownEvictedCount++;
        }
    }

    private NotificationRecord record(AlertRule rule, WindowAggregate agg, AlertEventType kind,
                                      NotificationOrigin origin, NotificationDecision decision,
                                      int occurrence, int round, double observed,
                                      long decidedAtMs, String sourceEventId) {
        String id = rule.id() + ":" + agg.getFingerprint() + ":" + occurrence + ":" + round
                + ":" + origin.name().toLowerCase() + ":" + kind.name().toLowerCase()
                + ":" + agg.getWindowStartMs();
        return new NotificationRecord(id, rule.id(), agg.getFingerprint(), agg.getLabels(),
                kind, origin, decision, occurrence, round, agg.getWindowStartMs(), observed,
                decidedAtMs, sourceEventId);
    }

    static String keyOf(String ruleId, String fingerprint) {
        return ruleId + "|" + fingerprint;
    }

    /** 每次评估批次结束后调用：回收过期静默与老化跟踪条目。 */
    public synchronized void prune(long nowMs, long maxAgeMs) {
        silenceStore.pruneExpired(nowMs);
        flapTracker.prune(nowMs, maxAgeMs);
        // 冷却条目至少要活过一个完整冷却期，且按维度活跃度（而非通知时间）老化。
        long cutoff = nowMs - Math.max(maxAgeMs, cooldownMs);
        cooldowns.values().removeIf(ce -> ce.lastActivityWindowMs < cutoff);
    }

    /** 规则删除时回收其静默、冷却与抖动跟踪状态。 */
    public synchronized void onRuleDeleted(String ruleId) {
        silenceStore.removeByRule(ruleId);
        flapTracker.removeByRule(ruleId);
        cooldowns.keySet().removeIf(k -> k.startsWith(ruleId + "|"));
    }

    public List<NotificationRecord> records(String ruleId) {
        List<NotificationRecord> out = new ArrayList<>();
        for (NotificationRecord r : log) {
            if (r.ruleId().equals(ruleId)) {
                out.add(r);
            }
        }
        return out;
    }

    public List<NotificationRecord> allRecords() {
        return new ArrayList<>(log);
    }

    public List<FlapSnapshot> flapping() {
        return flapTracker.snapshots();
    }

    public SilenceStore silenceStore() {
        return silenceStore;
    }

    public FlapTracker flapTracker() {
        return flapTracker;
    }

    public synchronized int cooldownSize() {
        return cooldowns.size();
    }

    /** 因上限被淘汰的冷却条目累计数（可观测，不静默失效）。 */
    public synchronized long cooldownEvictedCount() {
        return cooldownEvictedCount;
    }
}
