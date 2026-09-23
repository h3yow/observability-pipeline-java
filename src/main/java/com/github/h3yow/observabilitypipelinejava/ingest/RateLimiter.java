package com.github.h3yow.observabilitypipelinejava.ingest;

import com.github.h3yow.observabilitypipelinejava.time.Clock;

/**
 * 固定窗口（按秒）上报速率限制器。速率配额每秒重置：压力回落后自动恢复。
 * 线程安全。limit=0 表示不限。
 */
public class RateLimiter {

    private final Clock clock;
    private final int limitPerSecond;
    private long currentSecond = -1L;
    private int used = 0;

    public RateLimiter(Clock clock, int limitPerSecond) {
        this.clock = clock;
        this.limitPerSecond = limitPerSecond;
    }

    /** 尝试通过一个请求；超过每秒配额返回 false。 */
    public synchronized boolean tryAcquire() {
        if (limitPerSecond <= 0) {
            return true;
        }
        long second = Math.floorDiv(clock.nowMillis(), 1000L);
        if (second != currentSecond) {
            currentSecond = second;
            used = 0;
        }
        if (used >= limitPerSecond) {
            return false;
        }
        used++;
        return true;
    }

    public int limit() {
        return limitPerSecond;
    }

    /** 当前秒内已用配额（用于观测）。 */
    public synchronized int currentSecondUsed() {
        long second = Math.floorDiv(clock.nowMillis(), 1000L);
        return second == currentSecond ? used : 0;
    }
}
