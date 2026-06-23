package com.sandeep.pipeline.parse;

import java.util.Collections;
import java.util.List;

/**
 * Outcome of parsing one batch: clean events that flow downstream, plus quarantined rows. Making the
 * split an explicit return type (rather than throwing) forces callers to acknowledge both outcomes.
 */
public record ParseResult(List<LogEvent> events, List<DeadLetterRecord> deadLetters) {

    public ParseResult {
        events = (events == null) ? List.of() : Collections.unmodifiableList(events);
        deadLetters = (deadLetters == null) ? List.of() : Collections.unmodifiableList(deadLetters);
    }

    public int successCount() {
        return events.size();
    }

    public int failureCount() {
        return deadLetters.size();
    }

    public boolean hasFailures() {
        return !deadLetters.isEmpty();
    }
}
