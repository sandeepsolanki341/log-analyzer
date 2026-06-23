package com.sandeep.pipeline.index;

import java.util.List;

/**
 * Models the per-item outcome of a bulk request. Elasticsearch bulk responses are not all-or-nothing
 * — each document succeeds or fails independently — so this carries the list of failures (successes
 * need no further action).
 */
public record BulkResponse(int attempted, List<ItemFailure> failures) {

    public int succeeded() {
        return attempted - failures.size();
    }

    public boolean hasFailures() {
        return !failures.isEmpty();
    }

    /**
     * One failed document within a bulk response.
     *
     * @param operation  the operation that failed (so it can be retried or dead-lettered).
     * @param httpStatus per-item status code (e.g. 429, 400, 503).
     * @param reason     human-readable error text.
     */
    public record ItemFailure(IndexOperation operation, int httpStatus, String reason) {

        /** 429 (too many requests) and 5xx are transient; other 4xx (mapping/parse) are permanent. */
        public boolean isRetryable() {
            return httpStatus == 429 || httpStatus >= 500;
        }
    }
}
