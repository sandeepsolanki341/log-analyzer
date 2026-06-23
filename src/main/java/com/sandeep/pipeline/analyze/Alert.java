package com.sandeep.pipeline.analyze;

import java.time.Instant;

/**
 * A detected anomaly raised by the {@link AnomalyDetector}. Alerts become synthetic events that flow
 * down the same pipeline into Elasticsearch, so they are searchable and timeline-aligned with the
 * logs that triggered them.
 *
 * @param at       when the alert was raised (event-time of the window, UTC).
 * @param rule     identifier of the rule that fired, e.g. "ERROR_RATE_SPIKE".
 * @param subject  the entity the alert concerns (a service, an IP, or "GLOBAL").
 * @param message  human-readable description.
 * @param severity severity tier of the alert.
 */
public record Alert(Instant at, String rule, String subject, String message, SeverityBucket severity) {
}
