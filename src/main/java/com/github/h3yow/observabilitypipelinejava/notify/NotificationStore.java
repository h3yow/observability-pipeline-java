package com.github.h3yow.observabilitypipelinejava.notify;

import com.github.h3yow.observabilitypipelinejava.model.NotificationRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 通知判定记录存储（线程安全，追加有序）。 */
public class NotificationStore {

    private final CopyOnWriteArrayList<NotificationRecord> records = new CopyOnWriteArrayList<>();

    public void append(NotificationRecord record) {
        records.add(record);
    }

    public void appendAll(List<NotificationRecord> batch) {
        records.addAll(batch);
    }

    public List<NotificationRecord> list(String ruleId) {
        List<NotificationRecord> out = new ArrayList<>();
        for (NotificationRecord r : records) {
            if (ruleId == null || r.ruleId().equals(ruleId)) {
                out.add(r);
            }
        }
        return out;
    }

    public List<NotificationRecord> all() {
        return new ArrayList<>(records);
    }

    public int size() {
        return records.size();
    }
}
