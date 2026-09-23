package com.github.h3yow.observabilitypipelinejava.storage;

import java.util.List;

/** 样本存储：内存 + 本地追加日志（JSONL），重启可重放。 */
public interface SampleStore {
    /** 持久化并分配接收序号。 */
    StoredSample append(StoredSample sample);

    /** 重放全部已持久化样本（按接收序号）。 */
    List<StoredSample> replayAll();
}
