package com.github.h3yow.observabilitypipelinejava.notify;

import com.github.h3yow.observabilitypipelinejava.model.Silence;
import com.github.h3yow.observabilitypipelinejava.model.SilenceCreateStatus;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 静默/维护窗口存储（线程安全、有上限）。
 *
 * <p>设置结果以 {@link SilenceCreateStatus} 明确区分：
 * 非法区间、整体落在过去、完全重复、区间重叠、达到上限分别返回不同状态，
 * 不会默默生效或默默失效。到期静默随窗口推进由 {@link #pruneExpired} 回收；
 * 规则删除时由 {@link #removeByRule} 回收。</p>
 */
public class SilenceStore {

    /** 一次设置的结果：状态 + （成功时）条目。 */
    public record CreateResult(SilenceCreateStatus status, Silence silence) {
    }

    private final int maxSilences;
    private final Map<String, Silence> silences = new LinkedHashMap<>();

    public SilenceStore(int maxSilences) {
        if (maxSilences < 1) {
            throw new IllegalArgumentException("maxSilences must be >= 1");
        }
        this.maxSilences = maxSilences;
    }

    /**
     * 设置静默区间。校验顺序固定：非法区间 → 落在过去 → 重复 → 重叠 → 上限，
     * 同样的输入序列得到同样的结果序列。
     */
    public synchronized CreateResult create(String ruleId, Map<String, String> labelSelector,
                                            long startMs, long endMs, String reason, long nowMs) {
        if (ruleId == null || ruleId.isBlank()) {
            throw new IllegalArgumentException("ruleId is required");
        }
        Map<String, String> selector = labelSelector == null
                ? Map.of() : new TreeMap<>(labelSelector);
        if (endMs <= startMs) {
            return new CreateResult(SilenceCreateStatus.INVALID_RANGE, null);
        }
        if (endMs <= nowMs) {
            return new CreateResult(SilenceCreateStatus.IN_PAST, null);
        }
        String id = silenceId(ruleId, selector, startMs, endMs);
        if (silences.containsKey(id)) {
            return new CreateResult(SilenceCreateStatus.DUPLICATE, silences.get(id));
        }
        for (Silence s : silences.values()) {
            if (s.ruleId().equals(ruleId) && s.labelSelector().equals(selector)
                    && s.startMs() < endMs && startMs < s.endMs()) {
                return new CreateResult(SilenceCreateStatus.OVERLAP, s);
            }
        }
        if (silences.size() >= maxSilences) {
            return new CreateResult(SilenceCreateStatus.LIMIT_REACHED, null);
        }
        Silence silence = new Silence(id, ruleId, selector, startMs, endMs,
                reason == null ? "" : reason, nowMs);
        silences.put(id, silence);
        return new CreateResult(SilenceCreateStatus.CREATED, silence);
    }

    /** 确定性 ID：同规则 + 同选择器 + 同区间 ⇒ 同 ID，重复设置可识别。 */
    static String silenceId(String ruleId, Map<String, String> selector,
                            long startMs, long endMs) {
        return ruleId + "|" + selector + "|" + startMs + "|" + endMs;
    }

    /** 按规则过滤列出（ruleId 为 null 时列出全部），按规则+起点排序，顺序确定。 */
    public synchronized List<Silence> list(String ruleId) {
        List<Silence> out = new ArrayList<>();
        for (Silence s : silences.values()) {
            if (ruleId == null || s.ruleId().equals(ruleId)) {
                out.add(s);
            }
        }
        out.sort(java.util.Comparator.comparing(Silence::ruleId)
                .thenComparingLong(Silence::startMs)
                .thenComparing(Silence::id));
        return out;
    }

    /** 取消；返回是否真实移除了一条（不存在的 ID 返回 false，可区分）。 */
    public synchronized boolean cancel(String id) {
        return silences.remove(id) != null;
    }

    /** 判断某维度在某窗口起点是否被静默：任一覆盖该区间的匹配静默即生效。 */
    public synchronized boolean isSilenced(String ruleId, Map<String, String> labels,
                                           long windowStartMs) {
        for (Silence s : silences.values()) {
            if (s.ruleId().equals(ruleId) && s.covers(windowStartMs)
                    && s.matchesLabels(labels)) {
                return true;
            }
        }
        return false;
    }

    /** 回收 endMs &lt;= nowMs 的静默，返回回收条数。 */
    public synchronized int pruneExpired(long nowMs) {
        int before = silences.size();
        silences.values().removeIf(s -> s.endMs() <= nowMs);
        return before - silences.size();
    }

    /** 规则删除时回收其全部静默，返回回收条数。 */
    public synchronized int removeByRule(String ruleId) {
        int before = silences.size();
        silences.values().removeIf(s -> s.ruleId().equals(ruleId));
        return before - silences.size();
    }

    public synchronized int size() {
        return silences.size();
    }
}
