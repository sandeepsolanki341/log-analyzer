package com.sandeep.pipeline.parse;

import org.junit.jupiter.api.Test;

import com.sandeep.pipeline.parse.LogLevel;

import static org.junit.jupiter.api.Assertions.*;

class LogLevelTest {
    @Test
    void normalizesCommonAliases() {
        assertEquals(LogLevel.ERROR, LogLevel.fromRaw("SEVERE"));
        assertEquals(LogLevel.ERROR, LogLevel.fromRaw("err"));
        assertEquals(LogLevel.WARN, LogLevel.fromRaw(" Warning "));
        assertEquals(LogLevel.FATAL, LogLevel.fromRaw("panic"));
        assertEquals(LogLevel.UNKNOWN, LogLevel.fromRaw("zzz"));
        assertEquals(LogLevel.UNKNOWN, LogLevel.fromRaw(null));
        assertEquals(LogLevel.UNKNOWN, LogLevel.fromRaw("  "));
    }
}
