package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.domain.IngestResult;
import com.github.h3yow.observabilitypipelinejava.domain.IngestStatus;
import com.github.h3yow.observabilitypipelinejava.domain.MetricSample;
import com.github.h3yow.observabilitypipelinejava.domain.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.window.WindowMath;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 窗口对齐与聚合可复现性测试。
 */
public class WindowAggregationTest {

    private static final Logger log = LoggerFactory.getLogger(WindowAggregationTest.class);

    @Test
    void windowIsEpochAligned() {
        long size = 60_000L;
        assertEquals(120_000L, WindowMath.windowStart(179_999L, size));
        assertEquals(180_000L, WindowMath.windowStart(180_000L, size));
        // 负时间戳 floor 语义：连续不重叠
        assertEquals(-60_000L, WindowMath.windowStart(-1L, size));
        assertEquals(-60_000L, WindowMath.windowStart(-60_000L, size));
        log.info("window alignment verified: size={} [179999->120000, 180000->180000, -1->-60000]", size);
    }

    @Test
    void aggregationReproducibleDespiteOrder() {
        long base = WindowMath.windowStart(new com.github.h3yow.observabilitypipelinejava.time.TestClock().nowMillis(), 60_000L);
        // 样本偏移与值（窗口内偏移毫秒, 值）
        long[][] inOrder = {{1_000, 10}, {2_000, 20}, {3_000, 30}, {59_000, 40}};
        // 同批样本，乱序提交
        long[][] shuffled = {{59_000, 40}, {1_000, 10}, {3_000, 30}, {2_000, 20}};

        WindowAggregate a1 = build(base, inOrder);
        WindowAggregate a2 = build(base, shuffled);
        log.info("deterministic aggregation input ordered={} => count={} sum={} min={} max={} avg={}",
                java.util.Arrays.deepToString(inOrder), a1.count(), a1.sum(), a1.min(), a1.max(), a1.avg());
        log.info("deterministic aggregation input shuffled={} => count={} sum={} min={} max={} avg={}",
                java.util.Arrays.deepToString(shuffled), a2.count(), a2.sum(), a2.min(), a2.max(), a2.avg());

        assertEquals(a1.count(), a2.count());
        assertEquals(a1.sum(), a2.sum(), 0.0000001);
        assertEquals(a1.min(), a2.min(), 0d);
        assertEquals(a1.max(), a2.max(), 0d);
        assertEquals(a1.avg(), a2.avg(), 0.0000001);
        assertEquals(4L, a1.count());
        assertEquals(100d, a1.sum(), 0d);
        assertEquals(10d, a1.min(), 0d);
        assertEquals(40d, a1.max(), 0d);
        assertEquals(25d, a1.avg(), 0d);
    }

    @Test
    void lateAcceptedSampleRecomputesSameAggregate() {
        PipelineHarness h = new PipelineHarness();
        long ws = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        // 站在窗口中段接收样本，避免触发未来钟偏
        h.clock.setMillis(ws + 30_000L);

        List<MetricSample> samples = new ArrayList<>();
        samples.add(new MetricSample("m", Map.of("h", "a"), 10d, ws + 1_000));
        samples.add(new MetricSample("m", Map.of("h", "a"), 20d, ws + 2_000));
        for (MetricSample s : samples) {
            IngestResult r = h.ingest.ingest(s);
            log.info("initial input metric=m{{}} value={} ts={} => {} ({})",
                    "h=a", s.value(), s.timestamp(), r.status(), r.detail());
            assertEquals(IngestStatus.ACCEPTED, r.status());
        }

        // 推进到窗口封板但仍在宽限期，迟到样本应受理
        h.clock.setMillis(ws + h.props.getWindowSizeMillis() + 5_000L);
        MetricSample late = new MetricSample("m", Map.of("h", "a"), 30d, ws + 3_000);
        IngestResult lr = h.ingest.ingest(late);
        log.info("late input metric=m{{}} value={} ts={} at now={} => {} late={} ({})",
                "h=a", late.value(), late.timestamp(), h.clock.nowMillis(),
                lr.status(), lr.late(), lr.detail());
        assertEquals(IngestStatus.ACCEPTED_LATE, lr.status());
        assertTrue(lr.late());

        WindowAggregate agg = h.store.aggregatesForWindow("m", ws, h.props.getWindowSizeMillis()).get(0);
        log.info("aggregate after late arrival => count={} sum={} avg={} (expected 3/60/20)",
                agg.count(), agg.sum(), agg.avg());
        assertEquals(3L, agg.count());
        assertEquals(60d, agg.sum(), 0d);
        assertEquals(20d, agg.avg(), 0d);
        assertEquals(1, h.stats.acceptedLate());
    }

    private WindowAggregate build(long clockBase, long[][] tsValuePairs) {
        PipelineHarness h = new PipelineHarness();
        h.clock.setMillis(clockBase + 59_500L);
        for (long[] p : tsValuePairs) {
            IngestResult r = h.ingest.ingest(
                    new MetricSample("m", Map.of("k", "v"), (double) p[1], clockBase + p[0]));
            assertNotEquals(IngestStatus.REJECTED, r.status(), () -> "rejected: " + r.detail());
        }
        long ws = WindowMath.windowStart(clockBase, h.props.getWindowSizeMillis());
        return h.store.aggregatesForWindow("m", ws, h.props.getWindowSizeMillis()).get(0);
    }
}
