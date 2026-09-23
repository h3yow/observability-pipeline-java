package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.domain.IngestResult;
import com.github.h3yow.observabilitypipelinejava.domain.IngestStatus;
import com.github.h3yow.observabilitypipelinejava.domain.MetricSample;
import com.github.h3yow.observabilitypipelinejava.domain.RejectReason;
import com.github.h3yow.observabilitypipelinejava.domain.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.window.WindowMath;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发上报测试：多线程写入下不丢不重、聚合确定、受理/拒绝计数闭合。
 */
public class ConcurrentIngestTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestTest.class);

    @Test
    void concurrentWritesAreConsistentAndReproducible() throws Exception {
        PipelineHarness h = new PipelineHarness();
        long ws = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        h.clock.setMillis(ws + 30_000L);

        int threads = 8;
        int perThread = 500; // 每线程 500 个不同时间戳
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        long ts = ws + 1_000 + ((tid * perThread + i) % 58_000L);
                        double v = 1d;
                        MetricSample s = new MetricSample("conc", Map.of("h", "x"), v, ts);
                        IngestResult r = h.ingest.ingest(s);
                        if (r.status() == IngestStatus.ACCEPTED) {
                            accepted.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                            if (r.reason() != RejectReason.DUPLICATE) {
                                errors.add("unexpected reject: " + r.reason() + " " + r.detail());
                            }
                        }
                    }
                } catch (Exception e) {
                    errors.add(e.toString());
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        WindowAggregate agg = h.store.aggregatesForWindow("conc", ws, h.props.getWindowSizeMillis()).get(0);
        int totalAttempted = threads * perThread;
        int distinctTimestamps = 58_000; // ts 在 [ws+1000, ws+58999] 的去重空间
        int expectedAccepted = Math.min(totalAttempted, distinctTimestamps);
        int expectedDuplicates = totalAttempted - expectedAccepted;

        log.info("concurrent ingest attempted={} accepted={} rejected(duplicate)={} series={}",
                totalAttempted, accepted.get(), rejected.get(), h.store.activeSeriesCount());
        log.info("aggregate after concurrent writes => count={} sum={} (expected count={} sum={})",
                agg.count(), agg.sum(), expectedAccepted, (double) expectedAccepted);

        assertTrue(errors.isEmpty(), () -> "unexpected errors: " + errors);
        assertEquals(totalAttempted, accepted.get() + rejected.get(), "所有尝试必须有确定结论");
        assertEquals(expectedAccepted, agg.count(), "去重后样本数确定");
        assertEquals((double) expectedAccepted, agg.sum(), 0d, "聚合值可复现");
        assertEquals(expectedDuplicates, rejected.get(), "其余全部为重复拒绝");
        assertEquals(expectedDuplicates, h.stats.duplicate());
        assertEquals(expectedAccepted, h.stats.accepted());
    }

    @Test
    void concurrentDistinctSeriesRespectCardinalityLimit() throws Exception {
        PipelineHarness h = new PipelineHarness();
        long ws = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        h.clock.setMillis(ws + 30_000L);

        int threads = 8;
        int perThread = 20; // 共 160 个不同序列，超过 per-metric=3 与 global=10
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger cardinalityRejected = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        MetricSample s = new MetricSample("card",
                                Map.of("s", "t" + tid + "-" + i), 1d, ws + 1_000 + i);
                        IngestResult r = h.ingest.ingest(s);
                        if (r.status() == IngestStatus.ACCEPTED) {
                            accepted.incrementAndGet();
                        } else if (r.reason() == RejectReason.CARDINALITY_LIMIT
                                || r.reason() == RejectReason.GLOBAL_CARDINALITY_LIMIT) {
                            cardinalityRejected.incrementAndGet();
                        }
                    }
                } catch (Exception ignored) {
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        log.info("cardinality concurrent: accepted={} (must == per-metric limit 3), cardinalityRejected={}, activeSeries={}",
                accepted.get(), cardinalityRejected.get(), h.store.activeSeriesCount());
        assertEquals(h.props.getMaxSeriesPerMetric(), h.store.activeSeriesCountForMetric("card"),
                "并发下也不得突破单指标基数上限");
        assertEquals(h.props.getMaxSeriesPerMetric(), accepted.get(),
                "只有恰好等于上限数量的新序列被注册");
        assertEquals(threads * perThread, accepted.get() + cardinalityRejected.get(),
                "每条样本必须是受理或基数拒绝");
        assertFalse(h.stats.degradation().isEmpty(), "超限必须被观测为降级状态");
        log.info("degradation observed => {}", h.stats.degradation());
    }
}
