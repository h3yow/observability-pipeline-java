package com.github.h3yow.observabilitypipelinejava.alert;

import com.github.h3yow.observabilitypipelinejava.domain.Aggregation;
import com.github.h3yow.observabilitypipelinejava.domain.ComparisonOp;
import com.github.h3yow.observabilitypipelinejava.domain.WindowAggregate;

/**
 * 阈值判定工具：从聚合结果取观测值并按算子比较。纯函数，便于单测核对。
 */
public final class AlertEvaluator {

    private AlertEvaluator() {
    }

    /** 从聚合结果提取指定聚合方式的值。空窗口 COUNT 为 0，其余为 0。 */
    public static double extract(WindowAggregate agg, Aggregation aggregation) {
        return switch (aggregation) {
            case SUM -> agg.sum();
            case AVG -> agg.avg();
            case COUNT -> agg.count();
            case MIN -> agg.min();
            case MAX -> agg.max();
        };
    }

    /** 阈值比较。 */
    public static boolean compare(double observed, ComparisonOp op, double threshold) {
        return switch (op) {
            case GT -> observed > threshold;
            case GTE -> observed >= threshold;
            case LTE -> observed <= threshold;
            case LT -> observed < threshold;
            case EQ -> Double.compare(observed, threshold) == 0;
        };
    }
}
