package com.github.h3yow.observabilitypipelinejava.web;

import java.util.List;

/**
 * 设置静默窗口的请求体。
 *
 * @param id           静默 ID；为空时按内容推导（相同内容重复设置返回 REJECTED_DUPLICATE）
 * @param ruleId       作用规则 ID（必填）
 * @param fingerprints 限定的维度指纹；为空表示该规则全部维度
 * @param startMs      起始时刻（含）
 * @param endMs        结束时刻（不含）
 * @param reason       设置原因
 */
public record SilenceRequest(String id, String ruleId, List<String> fingerprints,
                             Long startMs, Long endMs, String reason) {
}
