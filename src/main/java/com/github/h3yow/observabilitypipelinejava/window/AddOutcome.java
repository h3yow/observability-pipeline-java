package com.github.h3yow.observabilitypipelinejava.window;

/**
 * 向窗口存储写入样本的底层结果。
 */
public enum AddOutcome {
    /** 写入成功（新样本） */
    ADDED,
    /** 完全重复（同窗口内相同时间戳+值） */
    DUPLICATE,
    /** 单指标序列基数超限 */
    SERIES_LIMIT,
    /** 全局序列基数超限 */
    GLOBAL_LIMIT
}
