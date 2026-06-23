package com.sandeep.pipeline.analyze;

import java.util.Collections;
import java.util.Map;

/**
 * Immutable, point-in-time snapshot of the {@link SlidingWindowAggregator}'s current window.
 * Consumed by the {@link AnomalyDetector}.
 */
public record WindowSnapshot(
        long windowSeconds,
        long totalEvents,
        long errorEvents,
        Map<String, Long> errorsByService,
        Map<String, Long> failedLoginsByIp,
        Map<String, Double> avgLatencyMsByService
) {
    public WindowSnapshot {
        errorsByService = safe(errorsByService);
        failedLoginsByIp = safe(failedLoginsByIp);
        avgLatencyMsByService = safe(avgLatencyMsByService);
    }

    public double errorRate() {
        return totalEvents == 0 ? 0.0 : (double) errorEvents / totalEvents;
    }

    private static <K, V> Map<K, V> safe(Map<K, V> m) {
        return m == null ? Map.of() : Collections.unmodifiableMap(Map.copyOf(m));
    }
}
