package com.github.h3yow.observabilitypipelinejava.window;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 序列键：指标名 + 规范化（按 key 排序、不可变）的标签集。
 * 标签集规范化后，标签提交顺序不影响序列归属与聚合结果。
 */
public record SeriesKey(String metric, SortedMap<String, String> labels) {

    public SeriesKey {
        // 防御性拷贝：TreeMap 不可变视图
        labels = (labels == null)
                ? java.util.Collections.unmodifiableSortedMap(new TreeMap<>())
                : java.util.Collections.unmodifiableSortedMap(new TreeMap<>(labels));
    }

    /** 以任意标签 Map 构造序列键。 */
    public static SeriesKey of(String metric, Map<String, String> labels) {
        return new SeriesKey(metric, new TreeMap<>(labels == null ? Map.of() : labels));
    }

    /** 稳定的维度字符串：metric{k=v,k2=v2}，用于展示与快照 key。 */
    public String dimensionString() {
        StringBuilder sb = new StringBuilder(metric).append('{');
        boolean first = true;
        for (Map.Entry<String, String> e : labels.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append(e.getKey()).append('=').append(e.getValue());
            first = false;
        }
        return sb.append('}').toString();
    }
}
