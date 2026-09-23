package com.github.h3yow.observabilitypipelinejava.guard;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * 序列身份：标签规范化、指纹计算、序列键。
 *
 * <p>规范化规则（保证同一逻辑序列只有一个指纹，结果跨进程稳定可复现）：
 * <ul>
 *   <li>键、值先做首尾空白裁剪；键不允许为空；</li>
 *   <li>null 值按空字符串处理；</li>
 *   <li>按键排序后，用 {@code key=value} 并以换行分隔形成规范串；</li>
 *   <li>指纹 = 规范串的 SHA-256 十六进制（空标签集合使用稳定常量 "none"）。</li>
 * </ul>
 */
public final class SeriesIdentity {

    /** 空标签集合的稳定指纹。 */
    public static final String EMPTY_FINGERPRINT = "none";

    private SeriesIdentity() {
    }

    /** 规范化标签：裁剪、null 归空串、按 key 排序。 */
    public static Map<String, String> normalize(Map<String, String> labels) {
        TreeMap<String, String> sorted = new TreeMap<>();
        if (labels != null) {
            for (Map.Entry<String, String> e : labels.entrySet()) {
                String k = e.getKey() == null ? "" : e.getKey().trim();
                String v = e.getValue() == null ? "" : e.getValue().trim();
                sorted.put(k, v);
            }
        }
        return sorted;
    }

    /** 标签指纹（规范化后哈希，跨进程稳定）。 */
    public static String fingerprint(Map<String, String> normalizedLabels) {
        if (normalizedLabels == null || normalizedLabels.isEmpty()) {
            return EMPTY_FINGERPRINT;
        }
        StringBuilder canonical = new StringBuilder();
        for (Map.Entry<String, String> e : normalizedLabels.entrySet()) {
            canonical.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        return sha256Hex(canonical.toString().getBytes(StandardCharsets.UTF_8));
    }

    /** 序列键：metric + 指纹。 */
    public static String seriesKey(String metric, String fingerprint) {
        return metric + "|" + fingerprint;
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
