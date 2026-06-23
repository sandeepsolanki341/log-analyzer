package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.analyze.Fingerprinter;
import com.sandeep.pipeline.parse.LogEvent;
import com.sandeep.pipeline.parse.LogLevel;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FingerprinterTest {

    private final Fingerprinter fp = new Fingerprinter();

    private LogEvent withStack(long id, String stack) {
        return new LogEvent(id, Instant.now(), LogLevel.ERROR, "svc", "x",
                null, null, null, null, stack, Map.of());
    }

    @Test
    void sameBugDifferentLineNumbers_sameFingerprint() {
        LogEvent a = withStack(1,
                "java.lang.NullPointerException\n\tat com.app.Foo.bar(Foo.java:142)\n\tat com.app.Baz.qux(Baz.java:88)");
        LogEvent b = withStack(2,
                "java.lang.NullPointerException\n\tat com.app.Foo.bar(Foo.java:999)\n\tat com.app.Baz.qux(Baz.java:12)");
        assertEquals(fp.fingerprint(a), fp.fingerprint(b));
    }

    @Test
    void differentException_differentFingerprint() {
        LogEvent a = withStack(1, "java.lang.NullPointerException\n\tat com.app.Foo.bar(Foo.java:142)");
        LogEvent b = withStack(2, "java.lang.IllegalStateException\n\tat com.app.Foo.bar(Foo.java:142)");
        assertNotEquals(fp.fingerprint(a), fp.fingerprint(b));
    }
}
