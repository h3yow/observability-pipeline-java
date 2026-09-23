package com.github.h3yow.observabilitypipelinejava.web;

import com.github.h3yow.observabilitypipelinejava.ingest.RateLimiter;
import com.github.h3yow.observabilitypipelinejava.service.MaintenanceService;
import com.github.h3yow.observabilitypipelinejava.service.PipelineStats;
import com.github.h3yow.observabilitypipelinejava.window.WindowStore;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 运维入口：状态观测与手动维护推进（本地验证用）。
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final PipelineStats stats;
    private final WindowStore windowStore;
    private final RateLimiter rateLimiter;
    private final MaintenanceService maintenanceService;

    public AdminController(PipelineStats stats, WindowStore windowStore, RateLimiter rateLimiter,
                           MaintenanceService maintenanceService) {
        this.stats = stats;
        this.windowStore = windowStore;
        this.rateLimiter = rateLimiter;
        this.maintenanceService = maintenanceService;
    }

    /** 管线状态：计数、拒绝原因、降级原因、基数、速率用量。 */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return stats.snapshot(windowStore.activeSeriesCount(),
                rateLimiter.currentSecondUsed(), rateLimiter.limit());
    }

    /** 手动触发一次维护推进（封板评估 + 回收 + 降级刷新）。 */
    @PostMapping("/tick")
    public Map<String, Object> tick() {
        int windows = maintenanceService.tick();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("evaluatedWindows", windows);
        m.put("stats", stats.snapshot(windowStore.activeSeriesCount(),
                rateLimiter.currentSecondUsed(), rateLimiter.limit()));
        return m;
    }
}
