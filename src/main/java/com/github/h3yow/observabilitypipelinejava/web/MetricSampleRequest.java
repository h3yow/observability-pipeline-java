package com.github.h3yow.observabilitypipelinejava.web;

import java.util.Map;

/** 指标上报请求体。timestampMs 缺省（null）时由服务端按当前时间补齐。 */
public record MetricSampleRequest(String metric,
                                  Map<String, String> labels,
                                  Long timestampMs,
                                  Double value,
                                  String sourceId) {
}
