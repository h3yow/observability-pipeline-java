package com.github.h3yow.observabilitypipelinejava.notify;

import com.github.h3yow.observabilitypipelinejava.model.FlapSnapshot;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 抖动跟踪器：按 (规则, 维度) 记录观察窗口内"触发/恢复"切换次数。
 *
 * <ul>
 *   <li>切换次数在滑动观察窗口内达到阈值 ⇒ 进入降噪（damping），
 *       进入的当窗口即作为第一个稳定观察窗；</li>
 *   <li>降噪期间不再累计切换；连续稳定满足、或连续稳定不满足达到
 *       stableWindowsToExit 个窗口 ⇒ 退出降噪并清空切换历史；</li>
 *   <li>条目有上限：新建时若已满，淘汰 lastEvaluatedWindowMs 最旧者并计入
 *       evictedCount（可观测，不静默失效）；维度老化后由 prune 回收。</li>
 * </ul>
 * 全部以窗口起点为时间基准，同样的输入序列结论可复现。
 */
public class FlapTracker {

    /** 一次窗口评估后的抖动判定结论。 */
    public record FlapAssessment(boolean damping, boolean justEntered, boolean justExited,
                                 boolean exitedStableMatched, FlapSnapshot snapshot) {
    }

    private static final class Entry {
        final String ruleId;
        final String fingerprint;
        final Deque<Long> transitions = new ArrayDeque<>();
        boolean damping;
        int stableMatched;
        int stableUnmatched;
        long lastTransitionWindowMs = Long.MIN_VALUE;
        long lastEvaluatedWindowMs = Long.MIN_VALUE;

        Entry(String ruleId, String fingerprint) {
            this.ruleId = ruleId;
            this.fingerprint = fingerprint;
        }
    }

    private final long flapWindowMs;
    private final int transitionThreshold;
    private final int stableWindowsToExit;
    private final int maxTrackers;
    private final Map<String, Entry> entries = new HashMap<>();
    private long evictedCount;

    public FlapTracker(long flapWindowMs, int transitionThreshold,
                       int stableWindowsToExit, int maxTrackers) {
        if (flapWindowMs < 1 || transitionThreshold < 2 || stableWindowsToExit < 1
                || maxTrackers < 1) {
            throw new IllegalArgumentException(
                    "invalid flap tracker config: flapWindowMs=" + flapWindowMs
                            + ", transitionThreshold=" + transitionThreshold
                            + ", stableWindowsToExit=" + stableWindowsToExit
                            + ", maxTrackers=" + maxTrackers);
        }
        this.flapWindowMs = flapWindowMs;
        this.transitionThreshold = transitionThreshold;
        this.stableWindowsToExit = stableWindowsToExit;
        this.maxTrackers = maxTrackers;
    }

    /**
     * 记录一次状态切换（FIRED 或 RECOVERED 生命周期事件）。
     * 降噪期间的切换不再累计（退出只看连续稳定），保证退出条件可解释。
     */
    public synchronized void recordTransition(String key, String ruleId, String fingerprint,
                                              long windowStartMs) {
        Entry e = entry(key, ruleId, fingerprint);
        if (e.damping) {
            e.lastEvaluatedWindowMs = Math.max(e.lastEvaluatedWindowMs, windowStartMs);
            return;
        }
        if (e.lastTransitionWindowMs != windowStartMs) {
            e.transitions.addLast(windowStartMs);
            e.lastTransitionWindowMs = windowStartMs;
        }
    }

    /**
     * 用本窗口的匹配结果推进抖动判定。每个 (维度, 窗口) 只应调用一次。
     *
     * @param matched 本窗口条件是否满足
     */
    public synchronized FlapAssessment assess(String key, String ruleId, String fingerprint,
                                              boolean matched, long windowStartMs) {
        Entry e = entry(key, ruleId, fingerprint);
        // 只保留观察窗口 (windowStart - flapWindowMs, windowStart] 内的切换。
        long cutoff = windowStartMs - flapWindowMs;
        while (!e.transitions.isEmpty() && e.transitions.peekFirst() <= cutoff) {
            e.transitions.pollFirst();
        }

        boolean justEntered = false;
        boolean justExited = false;
        boolean exitedStableMatched = false;

        if (!e.damping && e.transitions.size() >= transitionThreshold) {
            e.damping = true;
            justEntered = true;
            e.stableMatched = 0;
            e.stableUnmatched = 0;
        }

        if (e.damping) {
            if (matched) {
                e.stableMatched += 1;
                e.stableUnmatched = 0;
            } else {
                e.stableUnmatched += 1;
                e.stableMatched = 0;
            }
            if (e.stableMatched >= stableWindowsToExit
                    || e.stableUnmatched >= stableWindowsToExit) {
                e.damping = false;
                justExited = true;
                exitedStableMatched = e.stableMatched >= stableWindowsToExit;
                e.transitions.clear();
                e.stableMatched = 0;
                e.stableUnmatched = 0;
            }
        }

        e.lastEvaluatedWindowMs = Math.max(e.lastEvaluatedWindowMs, windowStartMs);
        return new FlapAssessment(e.damping, justEntered, justExited, exitedStableMatched,
                snapshotOf(key, e));
    }

    private Entry entry(String key, String ruleId, String fingerprint) {
        Entry e = entries.get(key);
        if (e == null) {
            if (entries.size() >= maxTrackers) {
                evictOldest();
            }
            e = new Entry(ruleId, fingerprint);
            entries.put(key, e);
        }
        return e;
    }

    /** 淘汰评估窗口最旧的条目；键序兜底保证同输入下淘汰结果确定。 */
    private void evictOldest() {
        String victim = null;
        long oldest = Long.MAX_VALUE;
        List<String> keys = new ArrayList<>(entries.keySet());
        keys.sort(String::compareTo);
        for (String k : keys) {
            long t = entries.get(k).lastEvaluatedWindowMs;
            if (t < oldest) {
                oldest = t;
                victim = k;
            }
        }
        if (victim != null) {
            entries.remove(victim);
            evictedCount++;
        }
    }

    public synchronized List<FlapSnapshot> snapshots() {
        List<FlapSnapshot> out = new ArrayList<>();
        List<String> keys = new ArrayList<>(entries.keySet());
        keys.sort(String::compareTo);
        for (String k : keys) {
            out.add(snapshotOf(k, entries.get(k)));
        }
        return out;
    }

    private FlapSnapshot snapshotOf(String key, Entry e) {
        return new FlapSnapshot(e.ruleId, e.fingerprint, e.damping, e.transitions.size(),
                e.stableMatched, e.stableUnmatched, e.lastTransitionWindowMs,
                e.lastEvaluatedWindowMs);
    }

    /** 回收 lastEvaluatedWindowMs 早于 (nowMs - maxAgeMs) 的条目，返回回收数。 */
    public synchronized int prune(long nowMs, long maxAgeMs) {
        long cutoff = nowMs - maxAgeMs;
        int before = entries.size();
        entries.values().removeIf(e -> e.lastEvaluatedWindowMs < cutoff);
        return before - entries.size();
    }

    /** 规则删除时回收其全部跟踪条目，返回回收数。 */
    public synchronized int removeByRule(String ruleId) {
        int before = entries.size();
        entries.values().removeIf(e -> e.ruleId.equals(ruleId));
        return before - entries.size();
    }

    public synchronized int size() {
        return entries.size();
    }

    /** 因上限被淘汰的条目累计数（可观测，不静默失效）。 */
    public synchronized long evictedCount() {
        return evictedCount;
    }
}
