package com.github.h3yow.observabilitypipelinejava.web;

import com.github.h3yow.observabilitypipelinejava.model.AggregateFunction;
import com.github.h3yow.observabilitypipelinejava.model.Comparator;
import java.util.Map;

/** 规则创建/更新请求体。 */
public record RuleRequest(String id,
                          String metric,
                          Map<String, String> labelSelector,
                          AggregateFunction function,
                          Comparator comparator,
                          Double threshold,
                          Integer fireAfterWindows) {
}
