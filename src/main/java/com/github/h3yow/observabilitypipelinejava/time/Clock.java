package com.github.h3yow.observabilitypipelinejava.time;

/** 可替换时钟，测试中可手动推进，保证结果可复现。 */
public interface Clock {
    long nowMillis();
}
