package com.github.h3yow.observabilitypipelinejava.config;

import com.github.h3yow.observabilitypipelinejava.alert.AlertEngine;
import com.github.h3yow.observabilitypipelinejava.ingest.IngestService;
import com.github.h3yow.observabilitypipelinejava.ingest.RateLimiter;
import com.github.h3yow.observabilitypipelinejava.service.MaintenanceService;
import com.github.h3yow.observabilitypipelinejava.service.MetricsQueryService;
import com.github.h3yow.observabilitypipelinejava.service.PipelineStats;
import com.github.h3yow.observabilitypipelinejava.storage.AlertStateStore;
import com.github.h3yow.observabilitypipelinejava.storage.EventJournal;
import com.github.h3yow.observabilitypipelinejava.time.Clock;
import com.github.h3yow.observabilitypipelinejava.time.SystemClock;
import com.github.h3yow.observabilitypipelinejava.window.WindowStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 管线组件装配。
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    @Bean
    public Clock clock() {
        return new SystemClock();
    }

    @Bean
    public PipelineStats pipelineStats() {
        return new PipelineStats();
    }

    @Bean
    public WindowStore windowStore(PipelineProperties props) {
        return new WindowStore(props);
    }

    @Bean
    public RateLimiter rateLimiter(Clock clock, PipelineProperties props) {
        return new RateLimiter(clock, props.getMaxSamplesPerSecond());
    }

    @Bean
    public EventJournal eventJournal(PipelineProperties props) {
        return new EventJournal(props.getEventLogPath());
    }

    @Bean
    public AlertStateStore alertStateStore() {
        return new AlertStateStore();
    }

    @Bean
    public AlertEngine alertEngine(WindowStore windowStore, AlertStateStore stateStore,
                                   EventJournal journal, Clock clock) {
        return new AlertEngine(windowStore, stateStore, journal, clock);
    }

    @Bean
    public IngestService ingestService(WindowStore windowStore, RateLimiter rateLimiter,
                                       PipelineProperties props, Clock clock, PipelineStats stats) {
        return new IngestService(windowStore, rateLimiter, props, clock, stats);
    }

    @Bean
    public MaintenanceService maintenanceService(WindowStore windowStore, AlertEngine alertEngine,
                                                 PipelineProperties props, Clock clock, PipelineStats stats,
                                                 RateLimiter rateLimiter) {
        return new MaintenanceService(windowStore, alertEngine, props, clock, stats, rateLimiter);
    }

    @Bean
    public MetricsQueryService metricsQueryService(WindowStore windowStore) {
        return new MetricsQueryService(windowStore);
    }
}
