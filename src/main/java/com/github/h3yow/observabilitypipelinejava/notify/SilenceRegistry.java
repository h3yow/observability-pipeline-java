package com.github.h3yow.observabilitypipelinejava.notify;

import com.github.h3yow.observabilitypipelinejava.model.SilenceOutcome;
import com.github.h3yow.observabilitypipelinejava.model.SilenceResultCode;
import com.github.h3yow.observabilitypipelinejava.model.SilenceWindow;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 静默/维护窗口注册中心（线程安全，确定性）。
 *
 * <p>语义：
 * <ul>
 *   <li>静默只影响“对外通知”，不影响状态机判定；</li>
 *   <li>相同内容重复设置 → {@code REJECTED_DUPLICATE}；</li>
 *   <li>同规则同维度范围时间重叠 → {@code REJECTED_OVERLAP}；</li>
 *   <li>区间非法 / 整体在过去 / 达到容量上限分别返回
 *       {@code REJECTED_INVALID_RANGE} / {@code REJECTED_PAST} / {@code REJECTED_CAPACITY}；</li>
 *   <li>已结束的静默在每次写操作时被回收，不会无限增长。</li>
 * </ul>
 */
public class SilenceRegistry {

    private final int maxSilences;
    private final Map<String, SilenceWindow> silences = new LinkedHashMap<>();

    public SilenceRegistry(int maxSilences) {
        if (maxSilences < 1) {
            throw new IllegalArgumentException("maxSilences must be >= 1");
        }
        this.maxSilences = maxSilences;
    }

    /** 设置一个静默窗口，返回结构化结果（成功与各类拒绝可区分）。 */
    public synchronized SilenceOutcome create(SilenceWindow candidate, long nowMs) {
        if (candidate.ruleId() == null || candidate.ruleId().isBlank()) {
            return SilenceOutcome.rejected(SilenceResultCode.REJECTED_INVALID_RANGE,
                    "ruleId is required");
        }
        if (candidate.endMs() <= candidate.startMs() || candidate.startMs() < 0) {
            return SilenceOutcome.rejected(SilenceResultCode.REJECTED_INVALID_RANGE,
                    "invalid range [" + candidate.startMs() + "," + candidate.endMs() + ")");
        }
        if (candidate.endMs() <= nowMs) {
            return SilenceOutcome.rejected(SilenceResultCode.REJECTED_PAST,
                    "silence ends in the past: endMs=" + candidate.endMs() + " now=" + nowMs);
        }
        SilenceWindow existing = silences.get(candidate.id());
        if (existing != null) {
            return SilenceOutcome.rejected(SilenceResultCode.REJECTED_DUPLICATE,
                    "silence id already exists: " + candidate.id());
        }
        for (SilenceWindow other : silences.values()) {
            if (other.scopeIntersects(candidate) && other.timeOverlaps(candidate)) {
                return SilenceOutcome.rejected(SilenceResultCode.REJECTED_OVERLAP,
                        "overlaps existing silence " + other.id()
                                + " [" + other.startMs() + "," + other.endMs() + ")");
            }
        }
        purgeExpired(nowMs);
        if (silences.size() >= maxSilences) {
            return SilenceOutcome.rejected(SilenceResultCode.REJECTED_CAPACITY,
                    "silence capacity reached: " + maxSilences);
        }
        silences.put(candidate.id(), candidate);
        return new SilenceOutcome(SilenceResultCode.CREATED, candidate,
                "created " + candidate.id());
    }

    /** 取消静默；目标不存在时返回 NOT_FOUND（可区分）。 */
    public synchronized SilenceOutcome cancel(String id) {
        SilenceWindow removed = silences.remove(id);
        if (removed == null) {
            return SilenceOutcome.rejected(SilenceResultCode.NOT_FOUND,
                    "silence not found: " + id);
        }
        return new SilenceOutcome(SilenceResultCode.CANCELLED, removed,
                "cancelled " + id);
    }

    /** 给定规则+维度在指定时刻是否处于静默中。 */
    public synchronized boolean isSilenced(String ruleId, String fingerprint, long atMs) {
        for (SilenceWindow w : silences.values()) {
            if (w.ruleId().equals(ruleId) && w.covers(fingerprint) && w.activeAt(atMs)) {
                return true;
            }
        }
        return false;
    }

    /** 查询（可按规则过滤），结果按（规则, 起始时间, ID）排序，顺序确定。 */
    public synchronized List<SilenceWindow> list(String ruleId) {
        List<SilenceWindow> out = new ArrayList<>();
        for (SilenceWindow w : silences.values()) {
            if (ruleId == null || w.ruleId().equals(ruleId)) {
                out.add(w);
            }
        }
        out.sort(Comparator.comparing(SilenceWindow::ruleId)
                .thenComparingLong(SilenceWindow::startMs)
                .thenComparing(SilenceWindow::id));
        return out;
    }

    /** 回收已结束的静默，返回回收数量。 */
    public synchronized int purgeExpired(long nowMs) {
        int removed = 0;
        Iterator<Map.Entry<String, SilenceWindow>> it = silences.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().endMs() <= nowMs) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    /** 规则删除时回收其全部静默，返回回收数量。 */
    public synchronized int purgeRule(String ruleId) {
        int removed = 0;
        Iterator<Map.Entry<String, SilenceWindow>> it = silences.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().ruleId().equals(ruleId)) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    public synchronized int size() {
        return silences.size();
    }
}
