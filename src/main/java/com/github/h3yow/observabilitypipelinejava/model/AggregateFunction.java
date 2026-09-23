package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 窗口内支持的聚合函数。所有函数均满足交换律/结合律或显式按时间戳排序，
 * 保证乱序写入与重放得到相同结果。
 */
public enum AggregateFunction {
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX,
    /** 最新值：按（时间戳, 接收序号）取最大者，乱序下结果确定。 */
    LAST
}
