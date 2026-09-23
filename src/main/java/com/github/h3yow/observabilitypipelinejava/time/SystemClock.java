package com.github.h3yow.observabilitypipelinejava.time;

/**
 * 基于 {@link System#currentTimeMillis()} 的生产时钟。
 */
public class SystemClock implements Clock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
