package com.github.h3yow.observabilitypipelinejava.service;

import com.github.h3yow.observabilitypipelinejava.alert.AlertEngine;
import com.github.h3yow.observabilitypipelinejava.config.PipelineProperties;
import com.github.h3yow.observabilitypipelinejava.ingest.RateLimiter;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
import com.github.h3yow.observabilitypipelinejava.window.WindowMath;
import com.github.h3yow.observabilitypipelinejava.window.WindowStore;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 维护服务（可被定时器或测试手动驱动，{@link #tick()} 确定性可重复）：
 * <ol>
 *   <li>对每个指标，按窗口顺序补评估所有"已封板（超过宽限期）但尚未评估"的窗口；</li>
 *   <li>回收超出保留期的窗口，释放序列基数计数；</li>
 *   <li>降级恢复：基数计数回落至限内、或速率秒窗口翻篇后清除对应降级标记。</li>
 * </ol>
 */
public class MaintenanceService {

    private final WindowStore store;
    private final AlertEngine alertEngine;
    private final PipelineProperties props;
    private final Clock clock;
    private final PipelineStats stats;
    private final RateLimiter rateLimiter;

    /** 指标 -> 已评估到的窗口起点（单调推进） */
    private final Map<String, Long> lastEvaluated = new HashMap<>();

    public MaintenanceService(WindowStore store, AlertEngine alertEngine, PipelineProperties props,
                              Clock clock, PipelineStats stats, RateLimiter rateLimiter) {
        this.store = store;
        this.alertEngine = alertEngine;
        this.props = props;
        this.clock = clock;
        this.stats = stats;
        this.rateLimiter = rateLimiter;
    }

    /** Spring 装配（无 RateLimiter 时用于纯单元构造）。 */
    public MaintenanceService(WindowStore store, AlertEngine alertEngine, PipelineProperties props,
                              Clock clock, PipelineStats stats) {
        this(store, alertEngine, props, clock, stats, null);
    }

    /**
     * 执行一次维护推进。返回本次新评估的窗口数。
     */
    public synchronized int tick() {
        long now = clock.nowMillis();
        long size = props.getWindowSizeMillis();
        long grace = props.getGracePeriodMillis();
        // 可评估的最新窗口：窗口结束 + 宽限期 <= now
        long latestSealedStart = WindowMath.windowStart(now - size - grace, size);
        // 保留期下界
        long oldestToKeep = WindowMath.windowStart(now - props.getRetentionMillis(), size);

        int evaluatedWindows = 0;
        // 评估驱动指标集合 = 存储中活跃指标 ∪ 规则定义的指标，
        // 保证规则在某指标数据出现前后任意时刻创建，其已封板窗口都会被评估。
        java.util.TreeSet<String> metricSet = new java.util.TreeSet<>(store.activeMetrics());
        for (com.github.h3yow.observabilitypipelinejava.domain.AlertRule rule : alertEngine.rules().values()) {
            metricSet.add(rule.metric());
        }
        for (String metric : metricSet) {
            long watermark = lastEvaluated.getOrDefault(metric, Long.MIN_VALUE);
            // 从 watermark 的下一个窗口开始；首个指标从保留期下界开始补齐
            long firstToEvaluate = (watermark == Long.MIN_VALUE) ? oldestToKeep : watermark + size;
            for (long ws = firstToEvaluate; ws <= latestSealedStart; ws += size) {
                if (ws >= oldestToKeep) {
                    alertEngine.evaluateWindow(metric, ws, size);
                    evaluatedWindows++;
                }
                lastEvaluated.put(metric, ws);
            }
        }

        // 回收过期窗口，释放基数
        store.evictBefore(oldestToKeep);

        refreshDegradation();
        return evaluatedWindows;
    }

    /** 根据当前压力刷新降级标记：压力回落即恢复正常判定。 */
    private void refreshDegradation() {
        int global = store.activeSeriesCount();
        if (global < props.getMaxSeriesGlobal()) {
            stats.clearDegradation(PipelineStats.DEGRADE_CARDINALITY_GLOBAL);
        }
        boolean anyMetricOver = false;
        for (String metric : store.activeMetrics()) {
            if (store.activeSeriesCountForMetric(metric) >= props.getMaxSeriesPerMetric()) {
                anyMetricOver = true;
            }
        }
        if (!anyMetricOver) {
            stats.clearDegradation(PipelineStats.DEGRADE_CARDINALITY_PER_METRIC);
        }
        if (rateLimiter != null
                && rateLimiter.currentSecondUsed() < Math.max(1, rateLimiter.limit())) {
            stats.clearDegradation(PipelineStats.DEGRADE_RATE);
        }
    }

    /** 生产环境定时驱动（每秒一次）。测试用 TestClock 时改由手动 tick。 */
    @Scheduled(fixedDelayString = "${observability.maintenance.interval-millis:1000}")
    public void scheduledTick() {
        tick();
    }
}
