package com.sandeep.pipeline.index;

import java.util.List;

/**
 * The single seam between the {@link BulkIndexer}'s reliability logic and the actual Elasticsearch
 * client. Keeping it an interface keeps the hard logic (batching, triage, backoff, dead-letter)
 * testable and dependency-free.
 */
public interface ElasticsearchTransport extends AutoCloseable {

    /**
     * Sends one bulk request and returns the per-item outcome. Implementations translate each
     * {@link IndexOperation} into an index action with {@code _id = op.id()} and {@code _index =
     * op.index()} (deterministic id =&gt; overwrite), collect per-item failures with status codes,
     * and throw {@link TransportException} only for whole-request failures.
     */
    BulkResponse bulk(List<IndexOperation> operations) throws TransportException;

    @Override
    default void close() throws Exception {
    }

    /** Thrown when an entire bulk request fails at the transport level (vs. per-item failures). */
    class TransportException extends Exception {
        private static final long serialVersionUID = 1L;
        public TransportException(String message, Throwable cause) {
            super(message, cause);
        }
        public TransportException(String message) {
            super(message);
        }
    }
}
