package com.github.h3yow.observabilitypipelinejava.time;

/**
 * 时钟抽象：生产用系统时钟，测试用可控时钟，保证时间相关结论可复现。
 */
public interface Clock {
    long nowMillis();
}
