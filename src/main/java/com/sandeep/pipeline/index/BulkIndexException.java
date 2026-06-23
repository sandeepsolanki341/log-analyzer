package com.sandeep.pipeline.index;

/**
 * Thrown when a batch cannot be fully accounted for (transport down and retries exhausted, or the
 * indexer was interrupted mid-flight). It propagates up so the checkpoint does NOT advance and the
 * batch is safely replayed next tick — duplicate-free thanks to deterministic document ids.
 */
public class BulkIndexException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public BulkIndexException(String message, Throwable cause) {
        super(message, cause);
    }

    public BulkIndexException(String message) {
        super(message);
    }
}
