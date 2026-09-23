package com.github.h3yow.observabilitypipelinejava.domain;

/**
 * 单条样本受理结果。迟到/乱序是可区分的受理结论；拒绝时给出原因。
 *
 * @param status       受理状态
 * @param reason       拒绝原因（未拒绝为 NONE）
 * @param windowStart  归属窗口起点（epoch 毫秒，拒绝时可能为 -1）
 * @param late         是否为迟到（窗口已封但仍在宽限期内）
 * @param outOfOrder   是否乱序（时间戳早于该序列已受理的最大时间戳，但仍被受理）
 * @param detail       人类可读说明
 */
public record IngestResult(IngestStatus status, RejectReason reason, long windowStart,
                           boolean late, boolean outOfOrder, String detail) {
}
