package com.github.h3yow.observabilitypipelinejava.engine;

import com.github.h3yow.observabilitypipelinejava.aggregate.WindowAggregator;
import com.github.h3yow.observabilitypipelinejava.alert.AlertEvaluator;
import com.github.h3yow.observabilitypipelinejava.guard.CardinalityGuard;
import com.github.h3yow.observabilitypipelinejava.model.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.model.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
import java.util.List;

/**
 * 推进器：封存到期窗口 -&gt; 按序触发告警判定 -&gt; 驱逐过期窗口并释放基数配额。
 * 由定时任务或测试手动调用，时间推进可复现。
 */
public class PipelineEngine {

    /** 一次推进的可观测结果。 */
    public record TickResult(long atMs, int sealedWindows, int alertEvents,
                             int evictedWindows, int deregisteredSeries) {
    }

    private final WindowAggregator aggregator;
    private final AlertEvaluator evaluator;
    private final CardinalityGuard cardinalityGuard;
    private final Clock clock;
    private volatile long lastTickMs = -1L;

    public PipelineEngine(WindowAggregator aggregator, AlertEvaluator evaluator,
                          CardinalityGuard cardinalityGuard, Clock clock) {
        this.aggregator = aggregator;
        this.evaluator = evaluator;
        this.cardinalityGuard = cardinalityGuard;
        this.clock = clock;
    }

    /** 执行一次推进。 */
    public TickResult tick() {
        long now = clock.nowMillis();
        List<WindowAggregate> sealed = aggregator.sealDueWindows(now);
        List<AlertEvent> events = evaluator.evaluate(sealed, now);
        WindowAggregator.EvictionResult eviction = aggregator.evictExpired(now);
        for (WindowAggregator.EvictedSeries es : eviction.evictedSeries()) {
            cardinalityGuard.deregister(es.metric(), es.fingerprint());
        }
        lastTickMs = now;
        return new TickResult(now, sealed.size(), events.size(),
                eviction.windowsEvicted(), eviction.evictedSeries().size());
    }

    public long lastTickMs() {
        return lastTickMs;
    }
}
