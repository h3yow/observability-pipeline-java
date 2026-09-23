package com.github.h3yow.observabilitypipelinejava.engine;

import org.springframework.scheduling.annotation.Scheduled;

/** 定时推进（仅 Spring 环境启用）。 */
public class PipelineScheduler {

    private final PipelineEngine engine;

    public PipelineScheduler(PipelineEngine engine) {
        this.engine = engine;
    }

    @Scheduled(fixedDelayString = "${observability.tick-ms:1000}")
    public void scheduledTick() {
        engine.tick();
    }
}
