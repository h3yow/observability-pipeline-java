package com.github.h3yow.observabilitypipelinejava.service;

import com.github.h3yow.observabilitypipelinejava.domain.WindowAggregate;
import com.github.h3yow.observabilitypipelinejava.window.WindowMath;
import com.github.h3yow.observabilitypipelinejava.window.WindowStore;

import java.util.ArrayList;
import java.util.List;

/**
 * 聚合查询服务：按指标 + 时间范围查询窗口聚合（只读，确定性）。
 */
public class MetricsQueryService {

    private final WindowStore store;

    public MetricsQueryService(WindowStore store) {
        this.store = store;
    }

    /**
     * 查询指标在 [fromMillis, toMillis) 内的全部窗口聚合。
     * 时间参数会被对齐到窗口边界；无数据窗口不输出。
     */
    public List<WindowAggregate> query(String metric, long fromMillis, long toMillis,
                                       long windowSizeMillis) {
        List<WindowAggregate> result = new ArrayList<>();
        long startWs = WindowMath.windowStart(fromMillis, windowSizeMillis);
        long endWs = WindowMath.windowStart(toMillis, windowSizeMillis);
        for (long ws = startWs; ws < endWs; ws += windowSizeMillis) {
            result.addAll(store.aggregatesForWindow(metric, ws, windowSizeMillis));
        }
        return result;
    }
}
