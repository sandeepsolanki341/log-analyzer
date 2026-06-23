package com.sandeep.pipeline.parse;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * The clean, canonical representation of a single log entry — the trusted counterpart to the dirty
 * {@code RawLogRecord}. Holding a {@code LogEvent} guarantees: UTC {@link Instant} timestamp, valid
 * {@link LogLevel}, trimmed strings, and an immutable extracted-fields map. Downstream layers consume
 * this without defensive re-parsing.
 *
 * @param sourceId        originating {@code app_logs.id}; carried through for the deterministic ES id.
 * @param timestamp       event time normalized to UTC. Never null.
 * @param level           canonical severity. Never null (worst case {@link LogLevel#UNKNOWN}).
 * @param service         originating service/module, trimmed; may be null.
 * @param message         message body, trimmed; may be null.
 * @param userId          associated user id, or null.
 * @param ip              source IP, or null.
 * @param statusCode      response status, or null.
 * @param latencyMs       request latency ms, or null.
 * @param stackTrace      raw stack trace, or null.
 * @param extractedFields key/values pulled from the message body. Never null; immutable.
 */
public record LogEvent(
        long sourceId,
        Instant timestamp,
        LogLevel level,
        String service,
        String message,
        Long userId,
        String ip,
        Integer statusCode,
        Long latencyMs,
        String stackTrace,
        Map<String, String> extractedFields
) {
    public LogEvent {
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp must not be null (normalize before constructing)");
        }
        if (level == null) {
            level = LogLevel.UNKNOWN;
        }
        extractedFields = (extractedFields == null)
                ? Map.of()
                : Collections.unmodifiableMap(Map.copyOf(extractedFields));
    }
}
