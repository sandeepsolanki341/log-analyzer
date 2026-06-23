package com.sandeep.pipeline.extract;

import java.sql.Timestamp;

/**
 * Immutable DTO representing one raw, unparsed row from the {@code app_logs} table.
 *
 * <p>A faithful 1:1 mirror of the database columns carrying <strong>no business logic</strong>: no
 * parsing, no normalization, no classification. Those belong downstream. Keeping this record "dumb"
 * lets extraction be tested in isolation. Nullable columns use boxed types so SQL {@code NULL}
 * round-trips as Java {@code null}.
 */
public record RawLogRecord(
        long id,
        Timestamp timestamp,
        String level,
        String service,
        String message,
        Long userId,
        String ip,
        Integer statusCode,
        Long latencyMs,
        String stackTrace
) {
}
