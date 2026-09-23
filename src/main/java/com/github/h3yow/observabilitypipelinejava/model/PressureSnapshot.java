package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Map;

/**
 * 系统压力与自观测快照：所有降级均可观测、原因可区分。 */
public record PressureSnapshot(long snapshotAtMs,
                               long acceptedTotal,
                               long acceptedLateTotal,
                               long duplicateTotal,
                               long rejectedTotal,
                               Map<IngestStatus, Long> rejectionsByStatus,
                               int currentSeriesCount,
                               Map<String, Integer> seriesPerMetric,
                               int maxSeriesPerMetric,
                               int maxGlobalSeries,
                               double currentIngestRatePerSec,
                               int rateLimitPerSec,
                               boolean metricCardinalityLimited,
                               boolean globalCardinalityLimited,
                               boolean rateLimited,
                               DegradeReason primaryReason) {
}
