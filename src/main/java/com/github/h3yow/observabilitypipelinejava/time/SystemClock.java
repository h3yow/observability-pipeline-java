package com.github.h3yow.observabilitypipelinejava.time;

public final class SystemClock implements Clock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
