package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.aggregate.WindowAggregator;
import com.github.h3yow.observabilitypipelinejava.alert.AlertEvaluator;
import com.github.h3yow.observabilitypipelinejava.alert.AlertRuleRegistry;
import com.github.h3yow.observabilitypipelinejava.alert.AlertStateStore;
import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import com.github.h3yow.observabilitypipelinejava.engine.PipelineEngine;
import com.github.h3yow.observabilitypipelinejava.guard.CardinalityGuard;
import com.github.h3yow.observabilitypipelinejava.guard.RateLimiter;
import com.github.h3yow.observabilitypipelinejava.ingest.IngestService;
import com.github.h3yow.observabilitypipelinejava.model.IngestResult;
import com.github.h3yow.observabilitypipelinejava.model.MetricSample;
import com.github.h3yow.observabilitypipelinejava.notify.NotificationGate;
import com.github.h3yow.observabilitypipelinejava.notify.NotificationStore;
import com.github.h3yow.observabilitypipelinejava.notify.SilenceRegistry;
import com.github.h3yow.observabilitypipelinejava.storage.FileSampleStore;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
import com.github.h3yow.observabilitypipelinejava.time.WindowTiming;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 确定性测试夹具：手动时钟 + 纯内存存储 + 手工 tick。 */
final class TestHarness {

    static final Logger LOG = LoggerFactory.getLogger(TestHarness.class);

    final MutableClock clock;
    final ObservabilityProperties props;
    final WindowTiming timing;
    final FileSampleStore store;
    final WindowAggregator aggregator;
    final CardinalityGuard cardinalityGuard;
    final RateLimiter rateLimiter;
    final IngestService ingest;
    final AlertRuleRegistry rules;
    final AlertStateStore alertStates;
    final AlertEvaluator evaluator;
    final SilenceRegistry silences;
    final NotificationStore notifications;
    final NotificationGate notificationGate;
    final PipelineEngine engine;

    TestHarness(ObservabilityProperties props, long startMs) {
        this.props = props;
        this.clock = new MutableClock(startMs);
        this.timing = new WindowTiming(props.getWindowSizeMs(), props.getLatenessMs(),
                props.getFutureSkewMs(), props.getRetentionMs());
        this.store = new FileSampleStore(props);
        this.aggregator = new WindowAggregator(timing);
        this.cardinalityGuard = new CardinalityGuard(props);
        this.rateLimiter = new RateLimiter(props.getRateLimitPerSec(), clock);
        this.ingest = new IngestService(props, timing, clock, store, aggregator,
                cardinalityGuard, rateLimiter);
        this.rules = new AlertRuleRegistry();
        this.alertStates = new AlertStateStore();
        this.evaluator = new AlertEvaluator(rules, alertStates);
        this.silences = new SilenceRegistry(props.getNotificationMaxSilences());
        this.notifications = new NotificationStore();
        this.notificationGate = new NotificationGate(silences, rules, props);
        this.engine = new PipelineEngine(aggregator, evaluator, notificationGate, notifications,
                cardinalityGuard, clock);
    }

    static ObservabilityProperties defaultProps() {
        ObservabilityProperties p = new ObservabilityProperties();
        p.setWindowSizeMs(10_000);
        p.setLatenessMs(20_000);
        p.setFutureSkewMs(5_000);
        p.setRetentionMs(100_000);
        p.setMaxSeriesPerMetric(3);
        p.setMaxGlobalSeries(10);
        p.setRateLimitPerSec(100_000);
        p.setJournalEnabled(false);
        p.setReplayOnStartup(false);
        return p;
    }

    static TestHarness create() {
        return new TestHarness(defaultProps(), 1_000_000_000_000L);
    }

    static TestHarness create(ObservabilityProperties props) {
        return new TestHarness(props, 1_000_000_000_000L);
    }

    long now() {
        return clock.nowMillis();
    }

    void advanceMs(long ms) {
        clock.advance(ms);
    }

    /** 时间对齐到窗口起点。 */
    long aligned(long offsetFromNowStart, long offsetMs) {
        long baseStart = timing.windowStartFor(clock.nowMillis());
        return baseStart + offsetFromNowStart * props.getWindowSizeMs() + offsetMs;
    }

    IngestResult send(String metric, Map<String, String> labels, Long timestampMs,
                      double value, String sourceId) {
        MetricSample sample = new MetricSample(metric, labels, timestampMs, value, sourceId);
        IngestResult r = ingest.ingest(sample);
        LOG.info("INGEST in={{{}, labels={}, ts={}, value={}, source={}} -> {} | seq={} | detail={}",
                metric, labels, timestampMs, value, sourceId,
                r.status(), r.receiveSeq(), r.detail());
        return r;
    }

    PipelineEngine.TickResult tick() {
        PipelineEngine.TickResult r = engine.tick();
        LOG.info("TICK at={} -> sealed={}, events={}, notifications={}, evictedWindows={}, deregisteredSeries={}",
                r.atMs(), r.sealedWindows(), r.alertEvents(), r.notifications(),
                r.evictedWindows(), r.deregisteredSeries());
        return r;
    }

    /**
     * 驱动窗口 i：在其开放期上报该窗口样本，然后把时钟推进到该窗口可封存并 tick。
     * 上报始终发生在“当前开放窗口”，避免自造迟到/封存竞态。
     */
    void driveWindow(String metric, Map<String, String> labels, int windowIndex, double value) {
        long w = props.getWindowSizeMs();
        long targetWindow = timing.windowStartFor(clock.nowMillis()) + windowIndex * w;
        long desiredNow = targetWindow + 1_000;
        if (desiredNow > clock.nowMillis()) {
            clock.advance(desiredNow - clock.nowMillis());
        }
        send(metric, labels, targetWindow + 1_000, value, "probe");
        long sealAt = targetWindow + w + props.getLatenessMs() + 1;
        clock.advance(sealAt - clock.nowMillis());
        tick();
    }

    /** 与生产代码一致的标签指纹。 */
    static String fingerprint(Map<String, String> labels) {
        return com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity.fingerprint(
                com.github.h3yow.observabilitypipelinejava.guard.SeriesIdentity
                        .normalize(labels));
    }

    static final class MutableClock implements Clock {
        private long nowMs;

        MutableClock(long startMs) {
            this.nowMs = startMs;
        }

        @Override
        public long nowMillis() {
            return nowMs;
        }

        void advance(long ms) {
            nowMs += ms;
        }
    }
}
