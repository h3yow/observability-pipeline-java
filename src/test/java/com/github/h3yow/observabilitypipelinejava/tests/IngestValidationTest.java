package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.domain.IngestResult;
import com.github.h3yow.observabilitypipelinejava.domain.IngestStatus;
import com.github.h3yow.observabilitypipelinejava.domain.MetricSample;
import com.github.h3yow.observabilitypipelinejava.domain.RejectReason;
import com.github.h3yow.observabilitypipelinejava.window.WindowMath;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 异常输入受理/拒绝的可区分原因测试，并核对计数。
 */
public class IngestValidationTest {

    private static final Logger log = LoggerFactory.getLogger(IngestValidationTest.class);

    private RejectReason check(PipelineHarness h, MetricSample s, IngestStatus expectedStatus,
                               RejectReason expectedReason) {
        IngestResult r = h.ingest.ingest(s);
        log.info("input metric={} labels={} value={} ts={} => status={} reason={} late={} ooo={} | {}",
                s.metric(), s.labels(), s.value(), s.timestamp(),
                r.status(), r.reason(), r.late(), r.outOfOrder(), r.detail());
        assertEquals(expectedStatus, r.status(), r.detail());
        assertEquals(expectedReason, r.reason(), r.detail());
        return r.reason();
    }

    @Test
    void invalidInputsGiveDistinctRejectReasons() {
        PipelineHarness h = new PipelineHarness();
        long now = h.clock.nowMillis();

        check(h, new MetricSample("  ", Map.of(), 1d, now), IngestStatus.REJECTED,
                RejectReason.EMPTY_METRIC_NAME);
        check(h, new MetricSample("m", Map.of("", "v"), 1d, now), IngestStatus.REJECTED,
                RejectReason.EMPTY_LABEL_KEY);
        check(h, new MetricSample("m", Map.of(), Double.NaN, now), IngestStatus.REJECTED,
                RejectReason.NON_FINITE_VALUE);
        check(h, new MetricSample("m", Map.of(), Double.POSITIVE_INFINITY, now),
                IngestStatus.REJECTED, RejectReason.NON_FINITE_VALUE);
        check(h, new MetricSample("m", Map.of(), 1d, now + 60_000L), IngestStatus.REJECTED,
                RejectReason.TIMESTAMP_IN_FUTURE);
        check(h, new MetricSample("m", Map.of(), 1d, now - h.props.getRetentionMillis() - 60_000L),
                IngestStatus.REJECTED, RejectReason.TIMESTAMP_TOO_OLD);

        log.info("rejected counters after invalid inputs => {}", h.stats.rejected());
        assertEquals(1L, h.stats.rejected().get(RejectReason.EMPTY_METRIC_NAME));
        assertEquals(1L, h.stats.rejected().get(RejectReason.EMPTY_LABEL_KEY));
        assertEquals(2L, h.stats.rejected().get(RejectReason.NON_FINITE_VALUE));
        assertEquals(1L, h.stats.rejected().get(RejectReason.TIMESTAMP_IN_FUTURE));
        assertEquals(1L, h.stats.rejected().get(RejectReason.TIMESTAMP_TOO_OLD));
        assertEquals(0L, h.stats.accepted());
    }

    @Test
    void duplicateIsRejectedDistinctlyAndCounted() {
        PipelineHarness h = new PipelineHarness();
        long ws = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        MetricSample s1 = new MetricSample("dup", Map.of("h", "1"), 5d, ws + 1_000);
        MetricSample s1Again = new MetricSample("dup", Map.of("h", "1"), 5d, ws + 1_000);
        // 同时间戳但不同值 -> 不是重复
        MetricSample s2 = new MetricSample("dup", Map.of("h", "1"), 6d, ws + 1_000);

        check(h, s1, IngestStatus.ACCEPTED, RejectReason.NONE);
        check(h, s1Again, IngestStatus.REJECTED, RejectReason.DUPLICATE);
        check(h, s2, IngestStatus.ACCEPTED, RejectReason.NONE);
        log.info("duplicate counter => {} (expected 1)", h.stats.duplicate());
        assertEquals(1L, h.stats.duplicate());
        assertEquals(2L, h.stats.accepted());
    }

    @Test
    void outOfOrderMarkedButAccepted() {
        PipelineHarness h = new PipelineHarness();
        long ws = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        h.clock.setMillis(ws + 30_000L);
        check(h, new MetricSample("ooo", Map.of(), 1d, ws + 20_000), IngestStatus.ACCEPTED,
                RejectReason.NONE);
        IngestResult r = h.ingest.ingest(new MetricSample("ooo", Map.of(), 2d, ws + 10_000));
        log.info("earlier-timestamp sample => status={} outOfOrder={} | {}",
                r.status(), r.outOfOrder(), r.detail());
        assertEquals(IngestStatus.ACCEPTED, r.status());
        assertTrue(r.outOfOrder());
        assertEquals(1L, h.stats.outOfOrder());
    }

    @Test
    void beyondGracePeriodIsRejectedWindowClosed() {
        PipelineHarness h = new PipelineHarness();
        long ws = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        // 推进到 窗口结束 + 宽限期之后
        h.clock.setMillis(ws + h.props.getWindowSizeMillis() + h.props.getGracePeriodMillis() + 1L);
        check(h, new MetricSample("late", Map.of(), 9d, ws + 1_000), IngestStatus.REJECTED,
                RejectReason.WINDOW_CLOSED);
    }
}
