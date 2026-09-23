package com.github.h3yow.observabilitypipelinejava.model;

import java.util.function.BiPredicate;

/**
 * 告警阈值比较符。 */
public enum Comparator {
    GT((v, t) -> v > t),
    GTE((v, t) -> v >= t),
    LT((v, t) -> v < t),
    LTE((v, t) -> v <= t),
    EQ((v, t) -> Double.compare(v, t) == 0),
    NEQ((v, t) -> Double.compare(v, t) != 0);

    private final BiPredicate<Double, Double> test;

    Comparator(BiPredicate<Double, Double> test) {
        this.test = test;
    }

    public boolean matches(double value, double threshold) {
        return test.test(value, threshold);
    }
}
