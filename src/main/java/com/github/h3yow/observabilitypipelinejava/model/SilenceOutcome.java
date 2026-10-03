package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 静默设置/取消的结构化结果。
 *
 * @param code    结果码（成功与各类拒绝可区分）
 * @param silence 涉及的静默窗口（创建/取消成功时为该窗口，否则为 null）
 * @param detail  判定依据（人读）
 */
public record SilenceOutcome(SilenceResultCode code, SilenceWindow silence, String detail) {

    public boolean ok() {
        return code == SilenceResultCode.CREATED || code == SilenceResultCode.CANCELLED;
    }

    public static SilenceOutcome rejected(SilenceResultCode code, String detail) {
        return new SilenceOutcome(code, null, detail);
    }
}
