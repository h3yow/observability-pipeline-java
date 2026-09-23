package com.github.h3yow.observabilitypipelinejava.storage;

import com.github.h3yow.observabilitypipelinejava.model.WindowAggregate;
import java.util.Map;

/**
 * 已受理并落盘/入内存的样本，带全局接收序号（决定 LAST 决胜与可追溯顺序）。
 */
public record StoredSample(long receiveSeq,
                           String metric,
                           Map<String, String> labels,
                           String fingerprint,
                           long timestampMs,
                           double value,
                           String sourceId,
                           long acceptedAtMs,
                           boolean late) implements WindowAggregate.StoredSampleView {
}
