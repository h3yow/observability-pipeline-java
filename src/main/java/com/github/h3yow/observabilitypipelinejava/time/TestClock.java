package com.github.h3yow.observabilitypipelinejava.time;

/**
 * 测试用可控时钟：初始值固定，可手动推进，默认固定为 2026-01-01T00:00:00Z。
 */
public class TestClock implements Clock {

    private long current;

    public TestClock() {
        this(1767225600000L);
    }

    public TestClock(long initialMillis) {
        this.current = initialMillis;
    }

    public void setMillis(long millis) {
        this.current = millis;
    }

    public void advanceMillis(long deltaMillis) {
        this.current += deltaMillis;
    }

    @Override
    public long nowMillis() {
        return current;
    }
}
