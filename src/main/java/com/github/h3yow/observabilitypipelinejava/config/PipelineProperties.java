package com.github.h3yow.observabilitypipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 管线可调参数（application.properties 可用 observability.* 覆盖）。
 */
@ConfigurationProperties(prefix = "observability")
public class PipelineProperties {

    /** 窗口长度（毫秒） */
    private long windowSizeMillis = 60_000L;
    /** 宽限期：窗口封窗后多久内仍接受迟到样本（毫秒） */
    private long gracePeriodMillis = 30_000L;
    /** 允许的未来钟偏（毫秒） */
    private long maxFutureSkewMillis = 5_000L;
    /** 单个指标的最大序列（标签集）基数 */
    private int maxSeriesPerMetric = 100;
    /** 全局最大序列基数 */
    private int maxSeriesGlobal = 1000;
    /** 每秒最大受理样本数（0 表示不限） */
    private int maxSamplesPerSecond = 0;
    /** 窗口数据保留时长（毫秒），超过后连同其序列计数一并回收 */
    private long retentionMillis = 300_000L;
    /** 告警事件本地落盘路径（空串表示只存内存） */
    private String eventLogPath = "";

    public long getWindowSizeMillis() { return windowSizeMillis; }
    public void setWindowSizeMillis(long v) { this.windowSizeMillis = v; }
    public long getGracePeriodMillis() { return gracePeriodMillis; }
    public void setGracePeriodMillis(long v) { this.gracePeriodMillis = v; }
    public long getMaxFutureSkewMillis() { return maxFutureSkewMillis; }
    public void setMaxFutureSkewMillis(long v) { this.maxFutureSkewMillis = v; }
    public int getMaxSeriesPerMetric() { return maxSeriesPerMetric; }
    public void setMaxSeriesPerMetric(int v) { this.maxSeriesPerMetric = v; }
    public int getMaxSeriesGlobal() { return maxSeriesGlobal; }
    public void setMaxSeriesGlobal(int v) { this.maxSeriesGlobal = v; }
    public int getMaxSamplesPerSecond() { return maxSamplesPerSecond; }
    public void setMaxSamplesPerSecond(int v) { this.maxSamplesPerSecond = v; }
    public long getRetentionMillis() { return retentionMillis; }
    public void setRetentionMillis(long v) { this.retentionMillis = v; }
    public String getEventLogPath() { return eventLogPath; }
    public void setEventLogPath(String v) { this.eventLogPath = v; }
}
