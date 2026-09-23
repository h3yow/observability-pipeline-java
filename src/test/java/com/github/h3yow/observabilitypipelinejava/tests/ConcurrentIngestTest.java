package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.IngestStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 并发写入下：受理计数精确、重复不重复计数、聚合值确定。 */
class ConcurrentIngestTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentIngestTest.class);

    @Test
    @DisplayName("N 线程并发上报不同样本：全部受理，COUNT 精确等于样本数")
    void concurrentDistinctSamplesAreExactlyCounted() throws Exception {
        TestHarness h = TestHarness.create();
        long now = h.now();
        int threads = 8;
        int perThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        ConcurrentLinkedQueue<String> unexpected = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // 同窗口、不同（线程,序号）时间偏移 => 唯一样本
                        long ts = now + (tid * perThread + i) % 9_000L;
                        var r = h.send("cpu", Map.of("host", "a"),
                                ts, tid * perThread + i, "s" + tid);
                        if (r.status() == IngestStatus.ACCEPTED) {
                            accepted.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                            unexpected.add(r.status() + ":" + r.detail());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdownNow();

        int total = threads * perThread;
        String fp = com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                .fingerprint(com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                        .normalize(Map.of("host", "a")));
        long w0 = h.timing.windowStartFor(now);
        var agg = h.aggregator.query("cpu", fp, w0);
        LOG.info("CONCURRENT distinct total={} accepted={} rejected={} aggregateCount={} sum={}",
                total, accepted.get(), rejected.get(), agg.count(), agg.valueOf(AggregateFunction.SUM));
        assertEquals(0, rejected.get(), "unexpected rejections: " + unexpected.stream().limit(5).toList());
        assertEquals(total, accepted.get());
        assertEquals(total, agg.count());

        // SUM 与线程交错无关，恒等于 0..total-1 之和
        double expectedSum = (long) (total - 1) * total / 2.0;
        assertEquals(expectedSum, agg.valueOf(AggregateFunction.SUM), 1e-6);
    }

    @Test
    @DisplayName("同一逻辑样本被多线程并发重复上报：恰好一次计入聚合，其余全部 DUPLICATE")
    void concurrentDuplicatesCollapseToOne() throws Exception {
        TestHarness h = TestHarness.create();
        long now = h.now();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        List<IngestStatus> other = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    var r = h.send("cpu", Map.of("host", "a"), now + 1_000, 42.0, "same-source");
                    if (r.status() == IngestStatus.ACCEPTED) {
                        accepted.incrementAndGet();
                    } else if (r.status() == IngestStatus.DUPLICATE) {
                        duplicates.incrementAndGet();
                    } else {
                        synchronized (other) {
                            other.add(r.status());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdownNow();

        String fp = com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                .fingerprint(com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                        .normalize(Map.of("host", "a")));
        var agg = h.aggregator.query("cpu", fp, h.timing.windowStartFor(now));
        LOG.info("CONCURRENT same-sample threads={} accepted={} duplicates={} other={} count={}",
                threads, accepted.get(), duplicates.get(), other, agg.count());
        assertEquals(1, accepted.get());
        assertEquals(threads - 1, duplicates.get());
        assertTrue(other.isEmpty());
        assertEquals(1L, agg.count());
        assertEquals(42.0, agg.valueOf(AggregateFunction.LAST), 0d);
    }
}
