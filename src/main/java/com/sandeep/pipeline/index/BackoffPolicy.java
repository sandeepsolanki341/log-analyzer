package com.sandeep.pipeline.index;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with full jitter and a hard attempt ceiling.
 *
 * <p>The most common retryable failure (HTTP 429) means Elasticsearch is overwhelmed — immediate
 * retries make it worse. Doubling the wait each attempt gives the cluster room; full jitter prevents
 * a thundering herd of retries firing in lockstep.
 */
public class BackoffPolicy {

    private final int maxAttempts;
    private final long baseDelayMs;
    private final long maxDelayMs;

    public BackoffPolicy() {
        this(4, 1_000L, 30_000L);
    }

    public BackoffPolicy(int maxAttempts, long baseDelayMs, long maxDelayMs) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** Full-jitter delay before the given 1-based retry attempt. */
    public long delayMillis(int attempt) {
        long exp = Math.min(maxDelayMs, baseDelayMs * (1L << Math.min(attempt - 1, 16)));
        return ThreadLocalRandom.current().nextLong(0, Math.max(1, exp) + 1);
    }

    /**
     * Sleeps for the computed delay.
     *
     * @throws InterruptedException if interrupted while sleeping; the caller is expected to abort and
     *                              let the batch replay (rather than swallowing the interrupt and
     *                              continuing to grind through retries during shutdown).
     */
    public void sleep(int attempt) throws InterruptedException {
        Thread.sleep(delayMillis(attempt));
    }
}
