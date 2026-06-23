package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.parse.LogEvent;

import java.util.Collections;
import java.util.Map;

/**
 * Output of the analyzer: a clean {@link LogEvent} decorated by the analysis stages. The
 * {@link #esId()} is the deterministic Elasticsearch document id — the linchpin of idempotency.
 */
public record AnalyzedEvent(
        LogEvent event,
        Classification classification,
        String fingerprint,
        Map<String, String> enrichment,
        String traceId,
        String esId,
        String targetIndex,
        boolean synthetic
) {
    public AnalyzedEvent {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        if (classification == null) {
            throw new IllegalArgumentException("classification must not be null");
        }
        if (fingerprint == null) {
            throw new IllegalArgumentException("fingerprint must not be null");
        }
        if (esId == null || esId.isBlank()) {
            throw new IllegalArgumentException("esId must not be null/blank");
        }
        if (targetIndex == null || targetIndex.isBlank()) {
            throw new IllegalArgumentException("targetIndex must not be null/blank");
        }
        enrichment = (enrichment == null) ? Map.of() : Collections.unmodifiableMap(Map.copyOf(enrichment));
    }
}
