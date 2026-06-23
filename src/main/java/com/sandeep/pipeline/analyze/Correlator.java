package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.parse.LogEvent;

/**
 * Stage 6 — Correlation. Resolves the correlation id tying related events into one logical trace,
 * normalizing across common key spellings so Elasticsearch can group by it at query time. ID
 * resolution only (no in-memory trace buffering), keeping the analyzer stateless on this axis.
 * Stateless and thread-safe.
 */
public class Correlator {

    private static final String[] TRACE_KEYS = {"traceId", "trace_id", "traceID", "trace"};
    private static final String[] SESSION_KEYS = {"sessionId", "session_id", "session"};

    public String correlate(LogEvent e) {
        String trace = firstPresent(e, TRACE_KEYS);
        return trace != null ? trace : firstPresent(e, SESSION_KEYS);
    }

    private String firstPresent(LogEvent e, String[] keys) {
        for (String k : keys) {
            String v = e.extractedFields().get(k);
            if (v != null && !v.isBlank()) {
                return v.strip();
            }
        }
        return null;
    }
}
