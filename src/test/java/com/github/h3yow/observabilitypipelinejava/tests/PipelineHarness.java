package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.alert.AlertEngine;
import com.github.h3yow.observabilitypipelinejava.config.PipelineProperties;
import com.github.h3yow.observabilitypipelinejava.ingest.IngestService;
import com.github.h3yow.observabilitypipelinejava.ingest.RateLimiter;
import com.github.h3yow.observabilitypipelinejava.service.MaintenanceService;
import com.github.h3yow.observabilitypipelinejava.service.MetricsQueryService;
import com.github.h3yow.observabilitypipelinejava.service.PipelineStats;
import com.github.h3yow.observabilitypipelinejava.storage.AlertStateStore;
import com.github.h3yow.observabilitypipelinejava.storage.EventJournal;
import com.github.h3yow.observabilitypipelinejava.time.TestClock;
import com.github.h3yow.observabilitypipelinejava.window.WindowStore;

/**
 * 纯单元测试夹具：手工装配全部组件，使用可控时钟，
 * 关闭 Spring 定时器，全部时间推进由测试显式驱动 => 结论可复现。
 */
public class PipelineHarness {

    public final TestClock clock;
    public final PipelineProperties props;
    public final PipelineStats stats;
    public final WindowStore store;
    public final RateLimiter rateLimiter;
    public final EventJournal journal;
    public final AlertStateStore stateStore;
    public final AlertEngine engine;
    public final IngestService ingest;
    public final MaintenanceService maintenance;
    public final MetricsQueryService query;

    public PipelineHarness() {
        this(defaultProps());
    }

    public PipelineHarness(PipelineProperties props) {
        this.props = props;
        this.clock = new TestClock();
        this.stats = new PipelineStats();
        this.store = new WindowStore(props);
        this.rateLimiter = new RateLimiter(clock, props.getMaxSamplesPerSecond());
        this.journal = new EventJournal(props.getEventLogPath());
        this.stateStore = new AlertStateStore();
        this.engine = new AlertEngine(store, stateStore, journal, clock);
        this.ingest = new IngestService(store, rateLimiter, props, clock, stats);
        this.maintenance = new MaintenanceService(store, engine, props, clock, stats, rateLimiter);
        this.query = new MetricsQueryService(store);
    }

    public static PipelineProperties defaultProps() {
        PipelineProperties p = new PipelineProperties();
        p.setWindowSizeMillis(60_000L);
        p.setGracePeriodMillis(10_000L);
        p.setMaxFutureSkewMillis(1_000L);
        p.setRetentionMillis(600_000L);
        p.setMaxSeriesPerMetric(3);
        p.setMaxSeriesGlobal(10);
        p.setMaxSamplesPerSecond(0);
        return p;
    }
}
