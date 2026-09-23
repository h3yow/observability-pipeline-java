package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.IngestStatus;
import com.github.h3yow.observabilitypipelinejava.model.WindowAggregate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 迟到/乱序/重复样本处理，以及同窗口聚合值的可复现性。 */
class WindowAggregationTest {

    private static final Logger LOG = LoggerFactory.getLogger(WindowAggregationTest.class);

    private record Input(long offset, double value, String source) {
    }

    /** 以乱序顺序发送同一组样本，返回当前窗口全部函数聚合值。 */
    private double[] runOrder(List<Input> order, long w0) {
        TestHarness h = new TestHarness(TestHarness.defaultProps(), w0 + 5_000);
        for (Input in : order) {
            h.send("cpu", Map.of("host", "a"), w0 + in.offset(), in.value(), in.source());
        }
        WindowAggregate agg = h.aggregator.query("cpu",
                com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                        .fingerprint(com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                                .normalize(Map.of("host", "a"))), w0);
        return new double[] {
                agg.valueOf(AggregateFunction.COUNT),
                agg.valueOf(AggregateFunction.SUM),
                agg.valueOf(AggregateFunction.AVG),
                agg.valueOf(AggregateFunction.MIN),
                agg.valueOf(AggregateFunction.MAX),
                agg.valueOf(AggregateFunction.LAST)
        };
    }

    @Test
    @DisplayName("乱序写入与顺序写入得到完全相同的聚合值（含 LAST 决胜）")
    void outOfOrderProducesSameAggregate() {
        long w0 = 1_000_000_000_000L;
        List<Input> asc = List.of(
                new Input(1_000, 10, "s1"),
                new Input(2_000, 20, "s1"),
                new Input(3_000, 30, "s1"),
                new Input(3_000, 99, "s2"),
                new Input(8_000, 40, "s1"));
        List<Input> shuffled = List.of(
                asc.get(4), asc.get(2), asc.get(0), asc.get(3), asc.get(1));
        double[] expected = {5, 199, 39.8, 10, 99, 40};

        double[] a = runOrder(asc, w0);
        double[] b = runOrder(shuffled, w0);
        LOG.info("AGG ascending ={}", java.util.Arrays.toString(a));
        LOG.info("AGG shuffled  ={}", java.util.Arrays.toString(b));
        assertArrayEquals(expected, a, 1e-9);
        assertArrayEquals(expected, b, 1e-9);
    }

    @Test
    @DisplayName("重复样本被去重；迟到样本在宽限期内补入 ACCEPTED_LATE")
    void duplicateAndLateHandling() {
        TestHarness h = TestHarness.create();
        long now = h.now();
        long w0 = h.timing.windowStartFor(now);

        var first = h.send("cpu", Map.of("host", "a"), w0 + 1_000, 10, "s1");
        // 完全相同的样本（含来源与数值）= 重复
        var dup = h.send("cpu", Map.of("host", "a"), w0 + 1_000, 10, "s1");
        // 同时间戳不同来源 = 新样本
        var otherSource = h.send("cpu", Map.of("host", "a"), w0 + 1_000, 10, "s2");
        // 同时间戳不同数值 = 新样本
        var otherValue = h.send("cpu", Map.of("host", "a"), w0 + 1_000, 11, "s1");

        assertEquals(IngestStatus.ACCEPTED, first.status());
        assertEquals(IngestStatus.DUPLICATE, dup.status());
        assertEquals(IngestStatus.ACCEPTED, otherSource.status());
        assertEquals(IngestStatus.ACCEPTED, otherValue.status());

        String fp = com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                .fingerprint(com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                        .normalize(Map.of("host", "a")));
        assertEquals(3L, h.aggregator.query("cpu", fp, w0).count());

        // 推进时间：窗口 0 关闭（w0+10s），但迟到宽限 20s；在 now=w0+25s 时补写 w0 窗口 => 迟到
        h.advanceMs(25_000);
        var late = h.send("cpu", Map.of("host", "a"), w0 + 5_000, 50, "s1");
        LOG.info("LATE result={}, detail={}", late.status(), late.detail());
        assertEquals(IngestStatus.ACCEPTED_LATE, late.status());
        assertEquals(4L, h.aggregator.query("cpu", fp, w0).count());

        // w0 窗口在 now=w0+25s 时尚不可封存（需 end+lateness=w0+30s）；tick 无封存
        var tickBefore = h.tick();
        assertEquals(0, tickBefore.sealedWindows());

        // 推进到 w0+35s：w0 可封存；封存后窗口冻结，迟到补写被拒绝为 TOO_OLD
        h.advanceMs(10_000);
        var tick = h.tick();
        assertEquals(1, tick.sealedWindows());
        var afterSeal = h.send("cpu", Map.of("host", "a"), w0 + 6_000, 60, "s1");
        LOG.info("AFTER-SEAL result={}, detail={}", afterSeal.status(), afterSeal.detail());
        assertEquals(IngestStatus.TOO_OLD, afterSeal.status());
        assertTrue(h.aggregator.query("cpu", fp, w0).isSealed());
        assertEquals(4L, h.aggregator.query("cpu", fp, w0).count());
    }

    @Test
    @DisplayName("聚合封存后冻结，结果可重复复现")
    void sealedAggregateIsFrozenAndReproducible() {
        long w0 = 1_000_000_000_000L;
        List<Input> inputs = new ArrayList<>(List.of(
                new Input(9_000, 5, "s1"),
                new Input(1_000, 7, "s1"),
                new Input(5_000, 9, "s1")));
        double[] first = runOrder(inputs, w0);
        inputs = new ArrayList<>(inputs);
        java.util.Collections.shuffle(inputs, new java.util.Random(42));
        double[] second = runOrder(inputs, w0);
        LOG.info("REPRODUCE run1={} run2={}", java.util.Arrays.toString(first),
                java.util.Arrays.toString(second));
        assertArrayEquals(first, second, 1e-9);
    }
}
