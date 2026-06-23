package com.sandeep.pipeline.parse;

import com.sandeep.pipeline.extract.RawLogRecord;

/**
 * A single row that could not be parsed/normalized, paired with the reason it failed. Quarantining
 * (rather than throwing) keeps one poison row from stalling the pipeline. A rising count is an
 * operational signal — usually a service changed its log format.
 *
 * @param raw    the original raw record.
 * @param reason short human-readable description of the failure.
 * @param cause  underlying exception, if any (may be null).
 */
public record DeadLetterRecord(RawLogRecord raw, String reason, Throwable cause) {
}
