package com.github.h3yow.observabilitypipelinejava.config;

import com.github.h3yow.observabilitypipelinejava.aggregate.WindowAggregator;
import com.github.h3yow.observabilitypipelinejava.alert.AlertEvaluator;
import com.github.h3yow.observabilitypipelinejava.alert.AlertRuleRegistry;
import com.github.h3yow.observabilitypipelinejava.alert.AlertStateStore;
import com.github.h3yow.observabilitypipelinejava.engine.PipelineEngine;
import com.github.h3yow.observabilitypipelinejava.engine.PipelineScheduler;
import com.github.h3yow.observabilitypipelinejava.guard.CardinalityGuard;
import com.github.h3yow.observabilitypipelinejava.guard.RateLimiter;
import com.github.h3yow.observabilitypipelinejava.ingest.IngestService;
import com.github.h3yow.observabilitypipelinejava.notify.NotificationGate;
import com.github.h3yow.observabilitypipelinejava.notify.NotificationStore;
import com.github.h3yow.observabilitypipelinejava.notify.SilenceRegistry;
import com.github.h3yow.observabilitypipelinejava.storage.FileSampleStore;
import com.github.h3yow.observabilitypipelinejava.storage.SampleStore;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
import com.github.h3yow.observabilitypipelinejava.time.SystemClock;
import com.github.h3yow.observabilitypipelinejava.time.WindowTiming;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Spring 装配：手工构造各组件，测试中可脱离 Spring 直接 new。 */
@Configuration
@EnableScheduling
public class PipelineConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "observability")
    public ObservabilityProperties observabilityProperties() {
        return new ObservabilityProperties();
    }

    @Bean
    public Clock clock() {
        return new SystemClock();
    }

    @Bean
    public WindowTiming windowTiming(ObservabilityProperties p) {
        return new WindowTiming(p.getWindowSizeMs(), p.getLatenessMs(),
                p.getFutureSkewMs(), p.getRetentionMs());
    }

    @Bean
    public SampleStore sampleStore(ObservabilityProperties p) {
        return new FileSampleStore(p);
    }

    @Bean
    public WindowAggregator windowAggregator(WindowTiming timing) {
        return new WindowAggregator(timing);
    }

    @Bean
    public CardinalityGuard cardinalityGuard(ObservabilityProperties p) {
        return new CardinalityGuard(p);
    }

    @Bean
    public RateLimiter rateLimiter(ObservabilityProperties p, Clock clock) {
        return new RateLimiter(p.getRateLimitPerSec(), clock);
    }

    @Bean
    public IngestService ingestService(ObservabilityProperties p, WindowTiming timing, Clock clock,
                                       SampleStore store, WindowAggregator aggregator,
                                       CardinalityGuard cardinalityGuard, RateLimiter rateLimiter) {
        IngestService service = new IngestService(p, timing, clock, store, aggregator,
                cardinalityGuard, rateLimiter);
        if (p.isReplayOnStartup()) {
            service.prime();
        }
        return service;
    }

    @Bean
    public AlertRuleRegistry alertRuleRegistry() {
        return new AlertRuleRegistry();
    }

    @Bean
    public AlertStateStore alertStateStore() {
        return new AlertStateStore();
    }

    @Bean
    public AlertEvaluator alertEvaluator(AlertRuleRegistry registry, AlertStateStore store) {
        return new AlertEvaluator(registry, store);
    }

    @Bean
    public SilenceRegistry silenceRegistry(ObservabilityProperties p) {
        return new SilenceRegistry(p.getNotificationMaxSilences());
    }

    @Bean
    public NotificationStore notificationStore() {
        return new NotificationStore();
    }

    @Bean
    public NotificationGate notificationGate(SilenceRegistry silenceRegistry,
                                             AlertRuleRegistry registry,
                                             ObservabilityProperties p) {
        return new NotificationGate(silenceRegistry, registry, p);
    }

    @Bean
    public PipelineEngine pipelineEngine(WindowAggregator aggregator, AlertEvaluator evaluator,
                                         NotificationGate notificationGate,
                                         NotificationStore notificationStore,
                                         CardinalityGuard cardinalityGuard, Clock clock) {
        return new PipelineEngine(aggregator, evaluator, notificationGate, notificationStore,
                cardinalityGuard, clock);
    }

    @Bean
    public PipelineScheduler pipelineScheduler(PipelineEngine engine) {
        return new PipelineScheduler(engine);
    }
}
