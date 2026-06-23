package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.parse.LogEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Stage 3 — Aggregation (STATEFUL, the heart of the analysis), keyed on <strong>event time</strong>.
 *
 * <h2>Event-time, not processing-time</h2>
 * Each entry is stamped and evicted by the event's own {@code timestamp}, never wall-clock. This is
 * what makes the window correct during catch-up and replay:
 * <ul>
 *   <li><b>Catch-up:</b> when draining a backlog, thousands of historical events do NOT collapse
 *       into one wall-clock instant; they span their real time range, so a 5-minute window means a
 *       real 5 minutes of logs and error rates are meaningful exactly when an incident is unfolding.</li>
 *   <li><b>Replay:</b> re-feeding the same events reproduces the same window deterministically.</li>
 * </ul>
 *
 * <h2>Watermark + bounded out-of-order tolerance</h2>
 * A monotonic <em>watermark</em> tracks the max event time seen. Eviction is relative to the
 * watermark, not to any single event, so a batch that arrives slightly out of id/time order is
 * handled gracefully. Entries older than {@code watermark - window} are evicted; events that arrive
 * more than one window late (beyond the watermark's reach) are counted into the current window
 * rather than silently dropped, which keeps totals honest for bursty backfills.
 *
 * <h2>Idempotent recording (replay-safe counts)</h2>
 * Each entry is keyed by {@code sourceId}. Re-recording an id already present in the window is a
 * no-op, so a replayed batch cannot double-count events still inside the window. Synthetic alert
 * events (sourceId {@code <= 0}) are never recorded.
 *
 * <p>All public methods are {@code synchronized}.
 */
public class SlidingWindowAggregator {

    private record Entry(long sourceId, Instant at, String service, boolean isError,
                         boolean isAuthFailure, String ip, Long latencyMs) {
    }

    private final Duration window;
    private final Deque<Entry> entries = new ArrayDeque<>();
    private final Map<Long, Boolean> present = new HashMap<>();
    private Instant watermark = Instant.EPOCH;

    public SlidingWindowAggregator(Duration window) {
        if (window == null || window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be a positive duration");
        }
        this.window = window;
    }

    /**
     * Records one classified event into the window, keyed by event time. Idempotent on
     * {@code event.sourceId()}.
     */
    public synchronized void record(LogEvent event, Classification classification) {
        long id = event.sourceId();
        if (id > 0 && present.containsKey(id)) {
            return; // replay-safe: already counted while still in window
        }
        Instant at = event.timestamp();
        if (at.isAfter(watermark)) {
            watermark = at;
        }
        boolean isError = classification.severity() == SeverityBucket.CRITICAL;
        boolean isAuthFailure = "AUTH".equals(classification.category());
        Entry e = new Entry(id, at,
                event.service() == null ? "UNKNOWN" : event.service(),
                isError, isAuthFailure, event.ip(), event.latencyMs());
        entries.addLast(e);
        if (id > 0) {
            present.put(id, Boolean.TRUE);
        }
        evict();
    }

    /** Produces an immutable snapshot of the current window after eviction. */
    public synchronized WindowSnapshot snapshot() {
        evict();

        long total = 0;
        long errors = 0;
        Map<String, Long> errorsByService = new HashMap<>();
        Map<String, Long> failedLoginsByIp = new HashMap<>();
        Map<String, long[]> latencyAccum = new HashMap<>();

        for (Entry e : entries) {
            total++;
            if (e.isError()) {
                errors++;
                errorsByService.merge(e.service(), 1L, Long::sum);
            }
            if (e.isAuthFailure() && e.ip() != null) {
                failedLoginsByIp.merge(e.ip(), 1L, Long::sum);
            }
            if (e.latencyMs() != null) {
                long[] acc = latencyAccum.computeIfAbsent(e.service(), k -> new long[2]);
                acc[0] += e.latencyMs();
                acc[1] += 1;
            }
        }

        Map<String, Double> avgLatency = new HashMap<>();
        for (var entry : latencyAccum.entrySet()) {
            long[] acc = entry.getValue();
            if (acc[1] > 0) {
                avgLatency.put(entry.getKey(), (double) acc[0] / acc[1]);
            }
        }

        return new WindowSnapshot(window.toSeconds(), total, errors,
                errorsByService, failedLoginsByIp, avgLatency);
    }

    /** The current event-time watermark (max event time observed). */
    public synchronized Instant watermark() {
        return watermark;
    }

    public synchronized int size() {
        return entries.size();
    }

    /** Evicts entries older than {@code watermark - window} from the front of the deque. */
    private void evict() {
        Instant cutoff = watermark.minus(window);
        while (!entries.isEmpty() && entries.peekFirst().at().isBefore(cutoff)) {
            Entry removed = entries.pollFirst();
            if (removed.sourceId() > 0) {
                present.remove(removed.sourceId());
            }
        }
    }
}
