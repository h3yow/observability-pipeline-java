package com.github.h3yow.observabilitypipelinejava.guard;

import com.github.h3yow.observabilitypipelinejava.time.Clock;

/**
 * 令牌桶限流：上报速率超限则拒绝，压力回落后自动恢复（令牌自然回补）。
 * 时钟可替换，测试中可手动推进，行为可复现。
 */
public class RateLimiter {

    private final int permitsPerSecond;
    private final Clock clock;
    private double tokens;
    private long lastRefillMs;
    private long lastAcquireMs = -1L;

    public RateLimiter(int permitsPerSecond, Clock clock) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        this.permitsPerSecond = permitsPerSecond;
        this.clock = clock;
        this.tokens = permitsPerSecond; // 冷启动允许一个桶的突发
        this.lastRefillMs = clock.nowMillis();
    }

    /** 尝试获取一个令牌。 */
    public synchronized boolean tryAcquire() {
        refill();
        if (tokens >= 1.0) {
            tokens -= 1.0;
            lastAcquireMs = clock.nowMillis();
            return true;
        }
        return false;
    }

    /** 此刻桶内令牌是否已见底（查询瞬间的限流状态）。 */
    public synchronized boolean isLimitedNow() {
        refill();
        return tokens < 1.0;
    }

    /** 当前估算速率（次/秒），供自观测。 */
    public synchronized double currentRatePerSec() {
        return permitsPerSecond - tokens >= 0 ? permitsPerSecond - tokens : 0d;
    }

    private void refill() {
        long now = clock.nowMillis();
        long elapsed = now - lastRefillMs;
        if (elapsed > 0) {
            tokens = Math.min(permitsPerSecond, tokens + elapsed * permitsPerSecond / 1000.0);
            lastRefillMs = now;
        }
    }
}
