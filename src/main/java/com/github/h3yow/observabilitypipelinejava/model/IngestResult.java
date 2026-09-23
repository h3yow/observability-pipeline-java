package com.github.h3yow.observabilitypipelinejava.model;

/**
 * 上报受理结果。任何输入都返回可区分的结果，绝不静默丢弃。
 *
 * @param status       受理/拒绝分类
 * @param accepted     是否最终写入聚合
 * @param seriesKey    序列键（metric + 标签指纹）；无法解析时为 null
 * @param windowStartMs 命中的窗口起点；无法定位时为 null
 * @param detail       人类可读的判定依据
 * @param receiveSeq   全局接收序号；拒绝时为 -1
 */
public record IngestResult(IngestStatus status,
                           boolean accepted,
                           String seriesKey,
                           Long windowStartMs,
                           String detail,
                           long receiveSeq) {
}
