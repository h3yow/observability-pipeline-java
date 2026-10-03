package com.github.h3yow.observabilitypipelinejava.model;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * 静默/维护窗口：某条规则（可限定到若干维度指纹）在 [startMs, endMs) 内
 * 不产生通知类告警事件；内部状态判定不受影响。
 *
 * @param id           静默 ID（唯一；内容相同时幂等，重复设置返回 REJECTED_DUPLICATE）
 * @param ruleId       作用规则
 * @param fingerprints 限定的维度指纹集合；空集表示该规则全部维度
 * @param startMs      起始（含）
 * @param endMs        结束（不含）
 * @param reason       设置原因（人读）
 * @param createdAtMs  创建时刻
 */
public record SilenceWindow(String id,
                            String ruleId,
                            Set<String> fingerprints,
                            long startMs,
                            long endMs,
                            String reason,
                            long createdAtMs) {

    public SilenceWindow {
        fingerprints = fingerprints == null ? Set.of()
                : java.util.Collections.unmodifiableSet(new TreeSet<>(fingerprints));
    }

    /** 是否覆盖给定维度。 */
    public boolean covers(String fp) {
        return fingerprints.isEmpty() || fingerprints.contains(fp);
    }

    /** 给定时刻是否生效。 */
    public boolean activeAt(long atMs) {
        return atMs >= startMs && atMs < endMs;
    }

    public SilenceStatus statusAt(long atMs) {
        if (atMs < startMs) {
            return SilenceStatus.SCHEDULED;
        }
        return atMs < endMs ? SilenceStatus.ACTIVE : SilenceStatus.EXPIRED;
    }

    /** 与另一静默在“规则 + 维度范围”上是否相交（空集合表示全规则，与任何范围相交）。 */
    public boolean scopeIntersects(SilenceWindow other) {
        if (!ruleId.equals(other.ruleId)) {
            return false;
        }
        if (fingerprints.isEmpty() || other.fingerprints.isEmpty()) {
            return true;
        }
        for (String fp : fingerprints) {
            if (other.fingerprints.contains(fp)) {
                return true;
            }
        }
        return false;
    }

    /** 时间区间是否重叠。 */
    public boolean timeOverlaps(SilenceWindow other) {
        return startMs < other.endMs && other.startMs < endMs;
    }

    /** 由内容推导的确定性 ID：相同输入重复设置时可被识别为重复。 */
    public static String deriveId(String ruleId, Set<String> fingerprints,
                                  long startMs, long endMs) {
        TreeSet<String> sorted = new TreeSet<>(fingerprints == null ? Set.of() : fingerprints);
        return "silence:" + ruleId + ":" + startMs + ":" + endMs + ":"
                + (sorted.isEmpty() ? "*" : String.join(",", sorted));
    }

    /** 便于日志/接口输出的维度摘要。 */
    public List<String> scopeList() {
        return List.copyOf(fingerprints);
    }
}
