package com.github.h3yow.observabilitypipelinejava.service;

import com.github.h3yow.observabilitypipelinejava.domain.RejectReason;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 管线运行计数与降级状态观测。所有计数线程安全；
 * 降级原因（基数/速率）可区分，压力回落后由维护服务清除。
 */
public class PipelineStats {

    /** 降级作用域标记：基数（单指标） */
    public static final String DEGRADE_CARDINALITY_PER_METRIC = "cardinality.per-metric";
    /** 降级作用域标记：基数（全局） */
    public static final String DEGRADE_CARDINALITY_GLOBAL = "cardinality.global";
    /** 降级作用域标记：速率 */
    public static final String DEGRADE_RATE = "rate";

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong acceptedLate = new AtomicLong();
    private final AtomicLong outOfOrder = new AtomicLong();
    private final AtomicLong duplicate = new AtomicLong();
    private final ConcurrentHashMap<RejectReason, AtomicLong> rejected = new ConcurrentHashMap<>();
    /** 降级原因 -> 该状态首次出现的时间戳（epoch 毫秒，0 表示用 -1 未知） */
    private final ConcurrentHashMap<String, Long> degradedSince = new ConcurrentHashMap<>();

    public PipelineStats() {
    }

    public void recordAccepted() {
        accepted.incrementAndGet();
    }

    public void recordAcceptedLate() {
        accepted.incrementAndGet();
        acceptedLate.incrementAndGet();
    }

    public void recordOutOfOrder() {
        outOfOrder.incrementAndGet();
    }

    public void recordDuplicate() {
        duplicate.incrementAndGet();
    }

    public void recordRejected(RejectReason reason) {
        rejected.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
    }

    public long accepted() {
        return accepted.get();
    }

    public long acceptedLate() {
        return acceptedLate.get();
    }

    public long outOfOrder() {
        return outOfOrder.get();
    }

    public long duplicate() {
        return duplicate.get();
    }

    /** 拒绝原因 -> 次数（保持插入顺序，便于阅读）。 */
    public Map<RejectReason, Long> rejected() {
        Map<RejectReason, Long> snapshot = new LinkedHashMap<>();
        rejected.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> snapshot.put(e.getKey(), e.getValue().get()));
        return snapshot;
    }

    /** 进入降级；重复标记保留最早时间，便于解释持续时长。 */
    private void mark(String scope, long nowMillis) {
        degradedSince.putIfAbsent(scope, nowMillis);
    }

    public void markCardinalityDegraded(String scope) {
        mark(scope, System.currentTimeMillis());
    }

    public void markRateDegraded() {
        mark(DEGRADE_RATE, System.currentTimeMillis());
    }

    /** 压力回落后清除对应降级标记。 */
    public void clearDegradation(String scope) {
        degradedSince.remove(scope);
    }

    /** 当前降级原因集合（空表示正常）：scope -> 自何时起（epoch 毫秒）。 */
    public Map<String, Long> degradation() {
        return new LinkedHashMap<>(degradedSince);
    }

    /** 计数与降级状态快照（用于 HTTP/日志输出）。 */
    public Map<String, Object> snapshot(int activeSeries, int rateUsed, int rateLimit) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("accepted", accepted.get());
        m.put("acceptedLate", acceptedLate.get());
        m.put("outOfOrder", outOfOrder.get());
        m.put("duplicate", duplicate.get());
        Map<String, Long> rejectedView = new LinkedHashMap<>();
        rejected().forEach((reason, count) -> rejectedView.put(reason.name(), count));
        m.put("rejected", rejectedView);
        m.put("activeSeries", activeSeries);
        m.put("rateUsedThisSecond", rateUsed);
        m.put("rateLimitPerSecond", rateLimit);
        m.put("degraded", new LinkedHashMap<>(degradedSince));
        return m;
    }

    public void reset() {
        accepted.set(0L);
        acceptedLate.set(0L);
        outOfOrder.set(0L);
        duplicate.set(0L);
        rejected.clear();
        degradedSince.clear();
    }
}
