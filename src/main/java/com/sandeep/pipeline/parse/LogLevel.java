package com.sandeep.pipeline.parse;

import java.util.Map;

/**
 * Canonical, closed vocabulary of log severities. Raw logs arrive with inconsistent strings
 * ({@code "ERROR"}, {@code "err"}, {@code "SEVERE"}, {@code "warn"}); folding them onto an enum makes
 * invalid levels impossible to hold downstream.
 */
public enum LogLevel {
    TRACE, DEBUG, INFO, WARN, ERROR, FATAL,
    /** Raw value present but unrecognized — kept distinct so it is observable. */
    UNKNOWN;

    private static final Map<String, LogLevel> ALIASES = Map.ofEntries(
            Map.entry("trace", TRACE), Map.entry("finest", TRACE),
            Map.entry("debug", DEBUG), Map.entry("fine", DEBUG), Map.entry("verbose", DEBUG),
            Map.entry("info", INFO), Map.entry("information", INFO), Map.entry("notice", INFO),
            Map.entry("warn", WARN), Map.entry("warning", WARN),
            Map.entry("error", ERROR), Map.entry("err", ERROR), Map.entry("severe", ERROR),
            Map.entry("fatal", FATAL), Map.entry("critical", FATAL), Map.entry("crit", FATAL),
            Map.entry("emergency", FATAL), Map.entry("panic", FATAL));

    public static LogLevel fromRaw(String raw) {
        if (raw == null) {
            return UNKNOWN;
        }
        String key = raw.strip().toLowerCase();
        if (key.isEmpty()) {
            return UNKNOWN;
        }
        return ALIASES.getOrDefault(key, UNKNOWN);
    }
}
