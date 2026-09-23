package com.github.h3yow.observabilitypipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.h3yow.observabilitypipelinejava.model.IngestStatus;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 异常输入必须给出可区分的拒绝结果，不静默丢弃。 */
class IngestValidationTest {

    private static final Logger LOG = LoggerFactory.getLogger(IngestValidationTest.class);

    @Test
    @DisplayName("非法指标名/数值/标签/时间戳分类拒绝原因可区分")
    void invalidInputsAreDistinguishable() {
        TestHarness h = TestHarness.create();
        long now = h.now();

        var badMetric = h.send("", Map.of(), now, 1.0, "s1");
        var nanValue = h.send("cpu", Map.of(), now, Double.NaN, "s1");
        var infValue = h.send("cpu", Map.of(), now, Double.POSITIVE_INFINITY, "s1");
        var labels21 = new java.util.HashMap<String, String>();
        for (int i = 0; i < 21; i++) {
            labels21.put("k" + i, "v" + i);
        }
        var tooManyLabels = h.send("cpu", labels21, now, 1.0, "s1");
        var badTs = h.send("cpu", Map.of(), 0L, 1.0, "s1");
        var future = h.send("cpu", Map.of(), now + 5_000 + 1, 1.0, "s1");
        var tooOld = h.send("cpu", Map.of(), now - 100_000 - 1, 1.0, "s1");
        var ok = h.send("cpu", Map.of(), now, 1.0, "s1");

        LOG.info("ASSERT badMetric={} nan={} inf={} labels={} badTs={} future={} tooOld={} ok={}",
                badMetric.status(), nanValue.status(), infValue.status(), tooManyLabels.status(),
                badTs.status(), future.status(), tooOld.status(), ok.status());

        assertEquals(IngestStatus.INVALID_METRIC, badMetric.status());
        assertEquals(IngestStatus.INVALID_VALUE, nanValue.status());
        assertEquals(IngestStatus.INVALID_VALUE, infValue.status());
        assertEquals(IngestStatus.INVALID_LABELS, tooManyLabels.status());
        assertEquals(IngestStatus.INVALID_TIMESTAMP, badTs.status());
        assertEquals(IngestStatus.TOO_FAR_FUTURE, future.status());
        assertEquals(IngestStatus.TOO_OLD, tooOld.status());
        assertEquals(IngestStatus.ACCEPTED, ok.status());
        assertFalse(tooOld.accepted());
        assertNotNull(ok.windowStartMs());
        assertTrue(ok.receiveSeq() > 0);
    }

    @Test
    @DisplayName("时间戳缺省由服务端补齐为当前时间")
    void missingTimestampFilledByServer() {
        TestHarness h = TestHarness.create();
        var r = h.send("cpu", Map.of("host", "a"), null, 2.0, "s1");
        LOG.info("ASSERT null-timestamp accepted into window {} (now={})",
                r.windowStartMs(), h.now());
        assertEquals(IngestStatus.ACCEPTED, r.status());
        assertEquals(h.timing.windowStartFor(h.now()), r.windowStartMs());
    }
}
