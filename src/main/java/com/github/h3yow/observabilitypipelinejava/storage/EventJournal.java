package com.github.h3yow.observabilitypipelinejava.storage;

import com.github.h3yow.observabilitypipelinejava.domain.AlertEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 告警事件日志：内存保存全部事件；若配置了落盘路径，则同时追加到本地文件
 * （JSON Lines，每行一个事件），用于本地持久核对。线程安全，事件序号全局单调。
 */
public class EventJournal {

    private final Path filePath;
    private final List<AlertEvent> events = new ArrayList<>();
    private long seq = 0L;

    public EventJournal(String filePath) {
        this.filePath = (filePath == null || filePath.isBlank()) ? null : Path.of(filePath);
    }

    /** 追加事件并赋予单调递增序号。 */
    public synchronized AlertEvent append(AlertEvent eventWithoutSeq) {
        long newSeq = ++seq;
        AlertEvent event = new AlertEvent(newSeq, eventWithoutSeq.ruleId(), eventWithoutSeq.metric(),
                eventWithoutSeq.labels(), eventWithoutSeq.type(), eventWithoutSeq.windowStart(),
                eventWithoutSeq.observedValue(), eventWithoutSeq.threshold(),
                eventWithoutSeq.reason(), eventWithoutSeq.eventTime());
        events.add(event);
        if (filePath != null) {
            try {
                Files.writeString(filePath, toJsonLine(event) + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                // 落盘失败不影响内存判定；以事件原因标注，保证可观测
                events.set(events.size() - 1, new AlertEvent(newSeq, event.ruleId(), event.metric(),
                        event.labels(), event.type(), event.windowStart(), event.observedValue(),
                        event.threshold(), event.reason() + " (file persist failed: " + e.getMessage() + ")",
                        event.eventTime()));
            }
        }
        return event;
    }

    public synchronized List<AlertEvent> all() {
        return new ArrayList<>(events);
    }

    /** 按规则 ID 过滤的事件。 */
    public synchronized List<AlertEvent> findByRule(String ruleId) {
        List<AlertEvent> result = new ArrayList<>();
        for (AlertEvent e : events) {
            if (e.ruleId().equals(ruleId)) {
                result.add(e);
            }
        }
        return result;
    }

    /** 下一个事件序号（不分配）。 */
    public synchronized long nextSeq() {
        return seq + 1;
    }

    public synchronized void clear() {
        events.clear();
        seq = 0L;
    }

    /** 极简 JSON 序列化（无第三方依赖），字段稳定便于人工核对。 */
    static String toJsonLine(AlertEvent e) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"eventSeq\":").append(e.eventSeq());
        sb.append(",\"ruleId\":\"").append(escape(e.ruleId())).append('"');
        sb.append(",\"metric\":\"").append(escape(e.metric())).append('"');
        sb.append(",\"labels\":").append(labelsJson(e.labels()));
        sb.append(",\"type\":\"").append(e.type().name()).append('"');
        sb.append(",\"windowStart\":").append(e.windowStart());
        sb.append(",\"observedValue\":").append(e.observedValue());
        sb.append(",\"threshold\":").append(e.threshold());
        sb.append(",\"reason\":\"").append(escape(e.reason())).append('"');
        sb.append(",\"eventTime\":").append(e.eventTime());
        return sb.append('}').toString();
    }

    private static String labelsJson(Map<String, String> labels) {
        TreeMap<String, String> sorted = new TreeMap<>(labels == null ? Map.of() : labels);
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> en : sorted.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append('"').append(escape(en.getKey())).append("\":\"")
                    .append(escape(en.getValue())).append('"');
            first = false;
        }
        return sb.append('}').toString();
    }

    private static String escape(String v) {
        if (v == null) {
            return "";
        }
        return v.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r");
    }
}
