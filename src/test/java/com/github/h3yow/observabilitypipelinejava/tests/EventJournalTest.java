package com.github.h3yow.observabilitypipelinejava.tests;

import com.github.h3yow.observabilitypipelinejava.domain.AlertEvent;
import com.github.h3yow.observabilitypipelinejava.domain.AlertEventType;
import com.github.h3yow.observabilitypipelinejava.storage.EventJournal;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 事件序号单调、内存可查、配置路径后追加落盘（JSON Lines）。
 */
public class EventJournalTest {

    private static final Logger log = LoggerFactory.getLogger(EventJournalTest.class);

    @Test
    void eventsAreSequencedAndPersisted() throws Exception {
        Path tmp = Files.createTempFile("alert-events", ".jsonl");
        Files.writeString(tmp, "");
        EventJournal journal = new EventJournal(tmp.toString());

        AlertEvent a = journal.append(new AlertEvent(0L, "r", "cpu", Map.of("h", "a"),
                AlertEventType.FIRING, 60_000L, 99d, 50d, "fired", 123L));
        AlertEvent b = journal.append(new AlertEvent(0L, "r", "cpu", Map.of("h", "a"),
                AlertEventType.RESOLVED, 120_000L, 1d, 50d, "resolved", 124L));
        log.info("appended events seq={} type={}, seq={} type={}",
                a.eventSeq(), a.type(), b.eventSeq(), b.type());

        assertEquals(1L, a.eventSeq());
        assertEquals(2L, b.eventSeq());
        List<String> lines = Files.readAllLines(tmp);
        log.info("persisted jsonl lines: {}", lines);
        assertEquals(2, lines.size());
        assertTrue(lines.get(0).contains("\"type\":\"FIRING\""));
        assertTrue(lines.get(0).contains("\"labels\":{\"h\":\"a\"}"));
        assertTrue(lines.get(1).contains("\"type\":\"RESOLVED\""));
        assertEquals(2, journal.findByRule("r").size());
        assertTrue(journal.findByRule("other").isEmpty());

        Files.deleteIfExists(tmp);
    }
}
