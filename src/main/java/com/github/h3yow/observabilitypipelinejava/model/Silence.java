package com.github.h3yow.observabilitypipelinejava.model;

import java.util.Map;

/**
 * 静默/维护窗口：某条规则（可选限定到部分维度）在 [startMs, endMs) 内不产生对外通知。
 * 判定按"被评估窗口的起点"是否落在区间内生效，保证可复现。
 *
 * @param labelSelector 维度选择器；空 Map 表示该规则全部维度
 */
public record Silence(String id,
                      String ruleId,
                      Map<String, String> labelSelector,
                      long startMs,
                      long endMs,
                      String reason,
                      long createdAtMs) {

    /** 窗口起点落在区间内即视为被静默。 */
    public boolean covers(long windowStartMs) {
        return windowStartMs >= startMs && windowStartMs < endMs;
    }

    /** 选择器匹配：样本标签须包含全部键值对；空选择器匹配所有维度。 */
    public boolean matchesLabels(Map<String, String> labels) {
        for (Map.Entry<String, String> e : labelSelector.entrySet()) {
            if (!e.getValue().equals(labels.get(e.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
