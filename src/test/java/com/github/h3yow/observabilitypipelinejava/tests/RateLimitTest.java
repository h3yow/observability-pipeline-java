package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.config.PipelineProperties;
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
 * 上报速率超限：拒绝 + 速率降级标记；进入下一秒配额重置后自动恢复。
 */
public class RateLimitTest {

    private static final Logger log = LoggerFactory.getLogger(RateLimitTest.class);

    @Test
    void rateLimitRejectsThenRecoversNextSecond() {
        PipelineProperties p = PipelineHarness.defaultProps();
        p.setMaxSamplesPerSecond(5);
        PipelineHarness h = new PipelineHarness(p);
        long ws = WindowMath.windowStart(h.clock.nowMillis(), h.props.getWindowSizeMillis());
        // 放在某秒开头，便于推进 1000ms 后精确翻秒
        h.clock.setMillis(ws + 30_000L);

        int accepted = 0;
        int limited = 0;
        for (int i = 0; i < 8; i++) {
            // 同序列不同时间戳，隔离出速率限制这一变量
            IngestResult r = h.ingest.ingest(
                    new MetricSample("rate", Map.of("host", "a"), 1d, ws + 1_000 + i));
            if (r.status() == IngestStatus.ACCEPTED) {
                accepted++;
            } else if (r.reason() == RejectReason.RATE_LIMITED) {
                limited++;
            }
            log.info("sample {} => {} {} ({})", i, r.status(), r.reason(), r.detail());
        }
        assertEquals(5, accepted, "每秒 5 个配额");
        assertEquals(3, limited, "超出 3 个被限流");
        assertTrue(h.stats.degradation().containsKey(
                com.github.h3yow.observabilitypipelinejava.service.PipelineStats.DEGRADE_RATE),
                "速率超限必须被观测为降级");

        // 进入下一秒：配额重置；tick 刷新降级标记后恢复。
        // 额外推进 1s 仍在同一窗口（窗口 60s），避免窗口边界导致的 WINDOW_CLOSED 干扰。
        h.clock.advanceMillis(1_000L);
        int windows = h.maintenance.tick();
        log.info("next-second tick => evaluatedWindows={} rateUsed={} degradation={}",
                windows, h.rateLimiter.currentSecondUsed(), h.stats.degradation());
        assertFalse(h.stats.degradation().containsKey(
                com.github.h3yow.observabilitypipelinejava.service.PipelineStats.DEGRADE_RATE),
                "压力回落（秒翻篇）后速率降级应清除");

        // 用当前时间戳上报（仍在开窗口内）
        IngestResult r = h.ingest.ingest(
                new MetricSample("rate", Map.of("i", "after"), 1d, h.clock.nowMillis()));
        log.info("sample after second rollover => {} {}", r.status(), r.reason());
        assertEquals(IngestStatus.ACCEPTED, r.status(), r.detail());
    }
}
