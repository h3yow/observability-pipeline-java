package com.github.h3yow.observabilitypipelinejava.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.observabilitypipelinejava.config.ObservabilityProperties;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于本地 JSONL 追加文件的样本存储。
 *
 * <p>语义：
 * <ul>
 *   <li>接收序号由存储在同一把锁内单调分配，落盘顺序即接收顺序，重放结果可复现；</li>
 *   <li>journalEnabled=false 时退化为纯内存（序号仍单调，但重启后不恢复）；</li>
 *   <li>{@link #prime()} 在启动时重放日志、把序号拨到历史最大值，返回全部历史样本；</li>
 *   <li>每行一个 JSON 对象（JSONL），损坏行跳过但计数，避免一条坏数据导致全量不可读。</li>
 * </ul>
 */
public class FileSampleStore implements SampleStore {

    private static final String JOURNAL_FILE = "samples.jsonl";

    private final ObjectMapper mapper = new ObjectMapper();
    private final boolean journalEnabled;
    private final Path journalPath;
    private final AtomicLong receiveSeq = new AtomicLong(0L);
    private final Object fileLock = new Object();

    public FileSampleStore(ObservabilityProperties properties) {
        this.journalEnabled = properties.isJournalEnabled();
        Path p = Path.of(properties.getJournalDir());
        this.journalPath = p.resolve(JOURNAL_FILE);
        if (journalEnabled) {
            try {
                Files.createDirectories(p);
                if (!Files.exists(journalPath)) {
                    Files.createFile(journalPath);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("cannot initialize journal at " + journalPath, e);
            }
        }
    }

    @Override
    public StoredSample append(StoredSample sample) {
        long seq = receiveSeq.incrementAndGet();
        StoredSample stored = new StoredSample(seq, sample.metric(), sample.labels(),
                sample.fingerprint(), sample.timestampMs(), sample.value(), sample.sourceId(),
                sample.acceptedAtMs(), sample.late());
        if (journalEnabled) {
            String line;
            try {
                line = mapper.writeValueAsString(stored);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to serialize sample", e);
            }
            synchronized (fileLock) {
                try (BufferedWriter writer = Files.newBufferedWriter(journalPath,
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                    writer.write(line);
                    writer.newLine();
                    writer.flush();
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to append journal", e);
                }
            }
        }
        return stored;
    }

    /** 启动重放：读取日志、推进序号，返回按接收序号排序的历史样本。 */
    public List<StoredSample> prime() {
        List<StoredSample> all = replayAll();
        long max = 0L;
        for (StoredSample s : all) {
            if (s.receiveSeq() > max) {
                max = s.receiveSeq();
            }
        }
        receiveSeq.set(max);
        return all;
    }

    @Override
    public List<StoredSample> replayAll() {
        if (!journalEnabled || !Files.exists(journalPath)) {
            return new ArrayList<>();
        }
        List<StoredSample> out = new ArrayList<>();
        long skipped = 0L;
        try {
            for (String line : Files.readAllLines(journalPath, StandardCharsets.UTF_8)) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    StoredSample s = mapper.readValue(line, StoredSample.class);
                    out.add(s);
                } catch (IOException badLine) {
                    skipped++;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read journal", e);
        }
        out.sort(java.util.Comparator.comparingLong(StoredSample::receiveSeq));
        if (skipped > 0) {
            // 损坏行不静默：落到标准错误（本地工具场景，无需引入日志门面依赖配置）。
            System.err.println("[FileSampleStore] skipped " + skipped + " corrupted journal line(s)");
        }
        return out;
    }
}
