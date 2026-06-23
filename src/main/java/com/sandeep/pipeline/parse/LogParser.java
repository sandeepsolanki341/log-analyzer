package com.sandeep.pipeline.parse;

import com.fasterxml.jackson.databind.JsonNode;
import com.sandeep.pipeline.extract.RawLogRecord;
import com.sandeep.pipeline.util.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses and normalizes {@link RawLogRecord} batches into clean {@link LogEvent}s.
 *
 * <h2>Mixed-format strategy ("handle all")</h2>
 * The {@code message} field is treated as potentially: (1) a JSON object, (2) a raw text log line,
 * or (3) nothing beyond the structured DB columns. The DB columns are always the baseline; message
 * parsing only enriches/overrides where it finds something.
 *
 * <h2>JSON parsing</h2>
 * JSON is parsed with Jackson (not a hand-rolled reader), so nested objects, arrays, escaping, and
 * non-string scalars are handled robustly. Nested objects are <em>flattened</em> to dotted keys
 * (e.g. {@code context.requestId}) so they remain searchable in Elasticsearch instead of forcing a
 * dead-letter. Only genuinely malformed JSON is quarantined.
 *
 * <h2>Timestamp normalization</h2>
 * All timestamps become UTC {@link Instant}s. A {@code java.sql.Timestamp} carries no zone; its
 * wall-clock value is interpreted in {@code sourceZoneForNaiveTimestamps}. <strong>Important:</strong>
 * for a MySQL {@code TIMESTAMP} column the driver already converts to the JDBC session zone, so the
 * JDBC URL should pin the session to UTC ({@code connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true})
 * and {@code sourceZoneForNaiveTimestamps} should be UTC. For a {@code DATETIME} column (zoneless),
 * set {@code sourceZoneForNaiveTimestamps} to the zone the service actually logs in. Getting this
 * pairing wrong shifts every timestamp by the offset, so it is configuration, not a guess.
 *
 * <h2>Fault isolation</h2>
 * Each row is parsed in its own try/catch; a row that cannot be parsed becomes a
 * {@link DeadLetterRecord} instead of aborting the batch.
 *
 * <p>Stateless and thread-safe; a single instance can be shared.
 */
public class LogParser {

    private static final Logger log = LoggerFactory.getLogger(LogParser.class);

    private static final Pattern KV_PAIR = Pattern.compile("(\\w+)=(\"[^\"]*\"|\\S+)");
    private static final Pattern EXCEPTION_TYPE =
            Pattern.compile("\\b([A-Za-z_][\\w.]*(?:Exception|Error|Throwable))\\b");
    private static final Pattern THREAD = Pattern.compile("\\[([^\\]]+)\\]");

    private final ZoneId sourceZoneForNaiveTimestamps;

    public LogParser() {
        this(ZoneOffset.UTC);
    }

    public LogParser(ZoneId sourceZoneForNaiveTimestamps) {
        if (sourceZoneForNaiveTimestamps == null) {
            throw new IllegalArgumentException("sourceZoneForNaiveTimestamps must not be null");
        }
        this.sourceZoneForNaiveTimestamps = sourceZoneForNaiveTimestamps;
    }

    public ParseResult parseBatch(List<RawLogRecord> batch) {
        List<LogEvent> events = new ArrayList<>();
        List<DeadLetterRecord> deadLetters = new ArrayList<>();

        if (batch == null || batch.isEmpty()) {
            return new ParseResult(events, deadLetters);
        }

        for (RawLogRecord raw : batch) {
            try {
                events.add(parseOne(raw));
            } catch (Exception e) {
                log.debug("Quarantining row id={} : {}", raw == null ? null : raw.id(), e.getMessage());
                deadLetters.add(new DeadLetterRecord(raw, describeFailure(e), e));
            }
        }

        if (!deadLetters.isEmpty()) {
            log.info("Parsed batch: {} ok, {} quarantined", events.size(), deadLetters.size());
        }
        return new ParseResult(events, deadLetters);
    }

    public LogEvent parseOne(RawLogRecord raw) {
        if (raw == null) {
            throw new IllegalArgumentException("raw record is null");
        }

        LogLevel level = LogLevel.fromRaw(raw.level());
        String service = blankToNull(raw.service());
        String message = raw.message() == null ? null : raw.message().strip();
        Long userId = raw.userId();
        String ip = blankToNull(raw.ip());
        Integer statusCode = raw.statusCode();
        Long latencyMs = raw.latencyMs();
        String stackTrace = blankToNull(raw.stackTrace());
        Map<String, String> extracted = new LinkedHashMap<>();

        if (message != null && !message.isEmpty()) {
            if (looksLikeJson(message)) {
                Map<String, String> json = parseJsonFlattened(message);
                if (level == LogLevel.UNKNOWN && json.containsKey("level")) {
                    level = LogLevel.fromRaw(json.get("level"));
                }
                if (service == null) {
                    service = blankToNull(firstNonNull(json.get("service"), json.get("logger")));
                }
                if (userId == null) {
                    userId = parseLongOrNull(firstNonNull(
                            json.get("user"), json.get("userId"), json.get("user_id")));
                }
                if (ip == null) {
                    ip = blankToNull(json.get("ip"));
                }
                if (statusCode == null) {
                    statusCode = parseIntOrNull(firstNonNull(
                            json.get("status"), json.get("statusCode"), json.get("status_code")));
                }
                if (latencyMs == null) {
                    latencyMs = parseLongOrNull(firstNonNull(
                            json.get("latency"), json.get("latencyMs"), json.get("latency_ms")));
                }
                extracted.putAll(json);
            } else {
                extractFromText(message, extracted);
                if (userId == null) {
                    userId = parseLongOrNull(extracted.get("user"));
                }
                if (statusCode == null) {
                    statusCode = parseIntOrNull(extracted.get("status"));
                }
            }
        }

        Instant ts = normalizeTimestamp(raw.timestamp(), extracted);

        return new LogEvent(raw.id(), ts, level, service, message,
                userId, ip, statusCode, latencyMs, stackTrace, extracted);
    }

    // ---------------------------------------------------------------------
    // Timestamp normalization
    // ---------------------------------------------------------------------

    private Instant normalizeTimestamp(Timestamp dbTs, Map<String, String> extracted) {
        if (dbTs != null) {
            LocalDateTime ldt = dbTs.toLocalDateTime();
            return ldt.atZone(sourceZoneForNaiveTimestamps).toInstant();
        }
        String raw = firstNonNull(extracted.get("ts"), extracted.get("timestamp"), extracted.get("time"));
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("no usable timestamp (column null and none in message)");
        }
        raw = raw.strip();

        if (raw.chars().allMatch(Character::isDigit)) {
            long n = Long.parseLong(raw);
            return raw.length() >= 13 ? Instant.ofEpochMilli(n) : Instant.ofEpochSecond(n);
        }
        try {
            return OffsetDateTime.parse(raw).toInstant();
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return LocalDateTime.parse(raw).atZone(sourceZoneForNaiveTimestamps).toInstant();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("unparseable timestamp: '" + raw + "'", e);
        }
    }

    // ---------------------------------------------------------------------
    // Text extraction
    // ---------------------------------------------------------------------

    private void extractFromText(String message, Map<String, String> out) {
        Matcher kv = KV_PAIR.matcher(message);
        while (kv.find()) {
            String val = kv.group(2);
            if (val.length() >= 2 && val.startsWith("\"") && val.endsWith("\"")) {
                val = val.substring(1, val.length() - 1);
            }
            out.put(kv.group(1), val);
        }
        Matcher ex = EXCEPTION_TYPE.matcher(message);
        if (ex.find()) {
            out.put("exception", ex.group(1));
        }
        Matcher th = THREAD.matcher(message);
        if (th.find()) {
            out.putIfAbsent("thread", th.group(1));
        }
    }

    // ---------------------------------------------------------------------
    // Robust JSON parsing via Jackson, flattened to dotted keys
    // ---------------------------------------------------------------------

    private static boolean looksLikeJson(String s) {
        String t = s.strip();
        return t.startsWith("{") && t.endsWith("}");
    }

    /**
     * Parses a JSON object and flattens it to {@code String -> String}. Nested objects produce dotted
     * keys ({@code a.b.c}); arrays are serialized to their JSON text. Malformed JSON throws (the row
     * is then dead-lettered by the caller).
     */
    static Map<String, String> parseJsonFlattened(String json) {
        try {
            JsonNode root = Json.MAPPER.readTree(json);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("not a JSON object");
            }
            Map<String, String> out = new LinkedHashMap<>();
            flatten("", root, out);
            return out;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("malformed JSON message: " + e.getMessage(), e);
        }
    }

    private static void flatten(String prefix, JsonNode node, Map<String, String> out) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
                flatten(key, e.getValue(), out);
            }
        } else if (node.isArray()) {
            out.put(prefix, node.toString());
        } else if (node.isNull()) {
            // skip nulls
        } else {
            out.put(prefix, node.asText());
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static String describeFailure(Exception e) {
        String msg = e.getMessage();
        return e.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    private static String blankToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.strip();
        return t.isEmpty() ? null : t;
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... vals) {
        for (T v : vals) {
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    private static Long parseLongOrNull(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Long.parseLong(s.strip());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Integer.parseInt(s.strip());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
