package com.github.h3yow.observabilitypipelinejava.config;

/**
 * 本地可观测性管线的全部可控参数（窗口、迟到、基数、速率、保留、存储）。
 * 可由 Spring 配置 observability.* 覆盖，测试中直接构造。
 */
public class ObservabilityProperties {
    private long windowSizeMs = 10_000;
    private long latenessMs = 30_000;
    private long futureSkewMs = 60_000;
    private long retentionMs = 3_600_000;
    private int maxSeriesPerMetric = 100;
    private int maxGlobalSeries = 1_000;
    private int rateLimitPerSec = 10_000;
    private int maxLabelCount = 20;
    private int maxLabelKeyLength = 128;
    private int maxLabelValueLength = 512;
    private String journalDir = "data/observability";
    private boolean journalEnabled = true;
    private boolean replayOnStartup = true;
    /** 触发通知冷却期（毫秒）；0 表示关闭冷却，行为与通知层引入前一致。 */
    private long notificationCooldownMs = 0;
    /** 抖动观察窗口（毫秒）；0 表示关闭抖动识别。 */
    private long notificationFlapWindowMs = 0;
    /** 观察窗口内状态转移达到该次数判定为抖动（>=2 才生效）。 */
    private int notificationFlapTransitionThreshold = 6;
    /** 降噪退出所需连续稳定满足/不满足的窗口数。 */
    private int notificationFlapExitStableWindows = 3;
    /** 静默窗口数量上限。 */
    private int notificationMaxSilences = 256;
    /** 通知跟踪器（规则×维度）数量上限。 */
    private int notificationMaxTrackers = 10_000;
    /** 通知跟踪器空闲回收时长（毫秒）。 */
    private long notificationTrackerIdleMs = 3_600_000;

    public long getWindowSizeMs() { return windowSizeMs; }
    public void setWindowSizeMs(long v) { this.windowSizeMs = v; }
    public long getLatenessMs() { return latenessMs; }
    public void setLatenessMs(long v) { this.latenessMs = v; }
    public long getFutureSkewMs() { return futureSkewMs; }
    public void setFutureSkewMs(long v) { this.futureSkewMs = v; }
    public long getRetentionMs() { return retentionMs; }
    public void setRetentionMs(long v) { this.retentionMs = v; }
    public int getMaxSeriesPerMetric() { return maxSeriesPerMetric; }
    public void setMaxSeriesPerMetric(int v) { this.maxSeriesPerMetric = v; }
    public int getMaxGlobalSeries() { return maxGlobalSeries; }
    public void setMaxGlobalSeries(int v) { this.maxGlobalSeries = v; }
    public int getRateLimitPerSec() { return rateLimitPerSec; }
    public void setRateLimitPerSec(int v) { this.rateLimitPerSec = v; }
    public int getMaxLabelCount() { return maxLabelCount; }
    public void setMaxLabelCount(int v) { this.maxLabelCount = v; }
    public int getMaxLabelKeyLength() { return maxLabelKeyLength; }
    public void setMaxLabelKeyLength(int v) { this.maxLabelKeyLength = v; }
    public int getMaxLabelValueLength() { return maxLabelValueLength; }
    public void setMaxLabelValueLength(int v) { this.maxLabelValueLength = v; }
    public String getJournalDir() { return journalDir; }
    public void setJournalDir(String v) { this.journalDir = v; }
    public boolean isJournalEnabled() { return journalEnabled; }
    public void setJournalEnabled(boolean v) { this.journalEnabled = v; }
    public boolean isReplayOnStartup() { return replayOnStartup; }
    public void setReplayOnStartup(boolean v) { this.replayOnStartup = v; }
    public long getNotificationCooldownMs() { return notificationCooldownMs; }
    public void setNotificationCooldownMs(long v) { this.notificationCooldownMs = v; }
    public long getNotificationFlapWindowMs() { return notificationFlapWindowMs; }
    public void setNotificationFlapWindowMs(long v) { this.notificationFlapWindowMs = v; }
    public int getNotificationFlapTransitionThreshold() { return notificationFlapTransitionThreshold; }
    public void setNotificationFlapTransitionThreshold(int v) { this.notificationFlapTransitionThreshold = v; }
    public int getNotificationFlapExitStableWindows() { return notificationFlapExitStableWindows; }
    public void setNotificationFlapExitStableWindows(int v) { this.notificationFlapExitStableWindows = v; }
    public int getNotificationMaxSilences() { return notificationMaxSilences; }
    public void setNotificationMaxSilences(int v) { this.notificationMaxSilences = v; }
    public int getNotificationMaxTrackers() { return notificationMaxTrackers; }
    public void setNotificationMaxTrackers(int v) { this.notificationMaxTrackers = v; }
    public long getNotificationTrackerIdleMs() { return notificationTrackerIdleMs; }
    public void setNotificationTrackerIdleMs(long v) { this.notificationTrackerIdleMs = v; }
}
