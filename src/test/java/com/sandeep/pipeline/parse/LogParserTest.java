package com.sandeep.pipeline.parse;

import com.sandeep.pipeline.extract.RawLogRecord;
import com.sandeep.pipeline.parse.LogEvent;
import com.sandeep.pipeline.parse.LogLevel;
import com.sandeep.pipeline.parse.LogParser;
import com.sandeep.pipeline.parse.ParseResult;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LogParserTest {

    private final LogParser parser = new LogParser(ZoneOffset.UTC);

    private RawLogRecord raw(long id, String level, String service, String message) {
        return new RawLogRecord(id, Timestamp.valueOf("2026-01-01 00:00:00"),
                level, service, message, null, null, null, null, null);
    }

    @Test
    void parsesNestedJsonIntoDottedFields_doesNotDeadLetter() {
        RawLogRecord r = raw(1, "INFO", "catalog",
                "{\"level\":\"info\",\"context\":{\"requestId\":\"abc\",\"q\":\"vitamin d\"}}");
        ParseResult result = parser.parseBatch(List.of(r));
        assertEquals(1, result.successCount());
        assertEquals(0, result.failureCount());
        LogEvent e = result.events().get(0);
        assertEquals("abc", e.extractedFields().get("context.requestId"));
        assertEquals("vitamin d", e.extractedFields().get("context.q"));
    }

    @Test
    void malformedJson_isDeadLettered_notCrashing() {
        RawLogRecord r = raw(2, "INFO", "svc", "{not valid json");
        ParseResult result = parser.parseBatch(List.of(r));
        // "{not valid json" doesn't end with } so it's treated as text, not JSON -> parses fine.
        // Use a string that looks like JSON but is malformed:
        RawLogRecord bad = raw(3, "INFO", "svc", "{\"a\": }");
        ParseResult r2 = parser.parseBatch(List.of(bad));
        assertEquals(1, r2.failureCount());
    }

    @Test
    void extractsKeyValuePairsFromText() {
        RawLogRecord r = raw(4, "ERROR", "checkout", "payment failed user=8821 traceId=xyz-1");
        LogEvent e = parser.parseOne(r);
        assertEquals(Long.valueOf(8821), e.userId());
        assertEquals("xyz-1", e.extractedFields().get("traceId"));
    }

    @Test
    void normalizesLevelAliases() {
        assertEquals(LogLevel.ERROR, parser.parseOne(raw(5, "SEVERE", "svc", "x")).level());
        assertEquals(LogLevel.WARN, parser.parseOne(raw(6, "warning", "svc", "x")).level());
    }

    @Test
    void oneBadRowDoesNotStallBatch() {
        RawLogRecord good = raw(7, "INFO", "svc", "ok");
        RawLogRecord bad = new RawLogRecord(8, null, "INFO", "svc", "no timestamp anywhere",
                null, null, null, null, null);
        ParseResult result = parser.parseBatch(List.of(good, bad));
        assertEquals(1, result.successCount());
        assertEquals(1, result.failureCount());
    }

    @Test
    void timestampNormalizedToUtcInstant() {
        LogEvent e = parser.parseOne(raw(9, "INFO", "svc", "x"));
        assertEquals(Instant.parse("2026-01-01T00:00:00Z"), e.timestamp());
    }
}
