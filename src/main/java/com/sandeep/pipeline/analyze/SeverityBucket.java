package com.sandeep.pipeline.analyze;

/**
 * Business-meaningful severity tier assigned by the {@link Classifier}, distinct from raw
 * {@code LogLevel}. LogLevel describes <em>what the log said</em>; this describes <em>how much we
 * should care</em>, which drives alerting and dashboards.
 */
public enum SeverityBucket {
    NORMAL, DEGRADED, CRITICAL
}
