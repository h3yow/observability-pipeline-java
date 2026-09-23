package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.IngestStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 本地 JSONL 日志重放：重启后窗口聚合值与接收序号可复现。 */
class JournalReplayTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalReplayTest.class);

    private TestHarness harnessWithJournal(Path dir, long startMs) {
        ObservabilityProperties p = TestHarness.defaultProps();
        p.setJournalEnabled(true);
        p.setReplayOnStartup(false);
        p.setJournalDir(dir.toString());
        return new TestHarness(p, startMs);
    }

    @Test
    @DisplayName("重启重放历史样本后，聚合值与重启前一致且序号单调续接")
    void replayReproducesAggregatesAndSequence(@TempDir Path dir) {
        long startMs = 5_000_000_000L;
        long w0;
        double sumBefore;
        long countBefore;
        long lastSeq;
        {
            TestHarness h = harnessWithJournal(dir, startMs + 2_000);
            w0 = h.timing.windowStartFor(h.now());
            h.send("cpu", Map.of("host", "a"), w0 + 1_000, 10, "s1");
            h.send("cpu", Map.of("host", "a"), w0 + 2_000, 20, "s1");
            var dup = h.send("cpu", Map.of("host", "a"), w0 + 1_000, 10, "s1");
            h.send("cpu", Map.of("host", "a"), w0 + 3_000, 30, "s1");
            assertEquals(IngestStatus.DUPLICATE, dup.status());
            var agg = h.aggregator.query("cpu", fp(), w0);
            countBefore = agg.count();
            sumBefore = agg.valueOf(AggregateFunction.SUM);
            lastSeq = h.store.replayAll().stream().mapToLong(s -> s.receiveSeq()).max().orElse(0);
            LOG.info("BEFORE RESTART count={} sum={} lastSeq={}", countBefore, sumBefore, lastSeq);
        }

        {
            // 模拟重启：新实例以同一目录 prime，时钟起点保持在同窗口开放期
            TestHarness h2 = harnessWithJournal(dir, startMs + 4_000);
            h2.ingest.prime();
            var agg = h2.aggregator.query("cpu", fp(), w0);
            LOG.info("AFTER RESTART count={} sum={}", agg.count(), agg.valueOf(AggregateFunction.SUM));
            assertEquals(countBefore, agg.count());
            assertEquals(sumBefore, agg.valueOf(AggregateFunction.SUM), 1e-9);

            // 基数注册也恢复：同指纹新样本不被当作新序列
            var more = h2.send("cpu", Map.of("host", "a"), w0 + 4_000, 40, "s1");
            assertEquals(IngestStatus.ACCEPTED, more.status());
            assertTrue(more.receiveSeq() > lastSeq, "sequence must continue after replay");
            assertEquals(sumBefore + 40,
                    h2.aggregator.query("cpu", fp(), w0).valueOf(AggregateFunction.SUM), 1e-9);
        }
    }

    private static String fp() {
        return com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                .fingerprint(com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                        .normalize(Map.of("host", "a")));
    }
}
