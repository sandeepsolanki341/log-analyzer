package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.parse.LogEvent;
import com.sandeep.pipeline.parse.LogLevel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowAggregatorTest {

    private final Classifier classifier = new Classifier();

    private LogEvent ev(long id, Instant ts, LogLevel lvl) {
        return new LogEvent(id, ts, lvl, "svc", "m", null, null, null, null, null, Map.of());
    }

    private LogEvent err(long id, Instant ts) {
        return new LogEvent(id, ts, LogLevel.ERROR, "svc", "boom", null, null, 500, null, null, Map.of());
    }

    @Test
    void eventTimeWindow_doesNotCollapseHistoricalEventsDuringCatchUp() {
        SlidingWindowAggregator agg = new SlidingWindowAggregator(Duration.ofMinutes(5));
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        // 10 events spanning 45 min of EVENT time, recorded in one burst (catch-up).
        for (int i = 0; i < 10; i++) {
            LogEvent e = ev(i + 1, base.plus(Duration.ofMinutes(i * 5L)), LogLevel.INFO);
            agg.record(e, classifier.classify(e));
        }
        // watermark = base+45m, window 5m, cutoff = base+40m: only minute-40 and minute-45 survive.
        assertEquals(2, agg.snapshot().totalEvents());
    }

    @Test
    void replay_doesNotDoubleCount() {
        SlidingWindowAggregator agg = new SlidingWindowAggregator(Duration.ofMinutes(5));
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        List<LogEvent> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(ev(i + 1, base.plusSeconds(i), LogLevel.INFO));
        }
        batch.forEach(e -> agg.record(e, classifier.classify(e)));
        assertEquals(5, agg.snapshot().totalEvents());
        batch.forEach(e -> agg.record(e, classifier.classify(e))); // replay
        assertEquals(5, agg.snapshot().totalEvents());
    }

    @Test
    void errorRate_isComputedOverWindow() {
        SlidingWindowAggregator agg = new SlidingWindowAggregator(Duration.ofMinutes(10));
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        for (int i = 0; i < 8; i++) {
            LogEvent e = ev(i + 1, base.plusSeconds(i), LogLevel.INFO);
            agg.record(e, classifier.classify(e));
        }
        for (int i = 8; i < 10; i++) {
            LogEvent e = err(i + 1, base.plusSeconds(i));
            agg.record(e, classifier.classify(e));
        }
        assertEquals(0.2, agg.snapshot().errorRate(), 1e-9);
    }

    @Test
    void watermark_isMaxEventTimeSeen() {
        SlidingWindowAggregator agg = new SlidingWindowAggregator(Duration.ofMinutes(5));
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        LogEvent a = ev(1, base, LogLevel.INFO);
        LogEvent b = ev(2, base.plusSeconds(30), LogLevel.INFO);
        agg.record(b, classifier.classify(b));
        agg.record(a, classifier.classify(a)); // out of order
        assertEquals(base.plusSeconds(30), agg.watermark());
    }
}
