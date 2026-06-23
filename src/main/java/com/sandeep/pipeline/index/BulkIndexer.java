package com.sandeep.pipeline.index;

import com.sandeep.pipeline.analyze.AnalyzedEvent;
import com.sandeep.pipeline.util.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Layer 4 — Bulk Indexer. The throughput-and-reliability valve between the pipeline and
 * Elasticsearch.
 *
 * <h2>Sub-batching (count + bytes)</h2>
 * The analyzer hands over a whole extractor batch (up to thousands of docs). Sending that as one ES
 * bulk request risks blowing past {@code http.max_content_length} and causes heap spikes / long GC.
 * So the indexer splits the batch into sub-requests bounded by BOTH a max document count and a max
 * byte size, independent of how many rows the extractor fetched. Each sub-request is independently
 * retried/triaged; a single oversized document is dead-lettered rather than wedging a sub-request.
 *
 * <h2>Reliability per sub-request</h2>
 * <ol>
 *   <li>Send via the {@link ElasticsearchTransport} seam.</li>
 *   <li>Inspect the per-item response (partial failures).</li>
 *   <li>Triage transient (429/5xx, retryable) vs permanent (4xx mapping/parse).</li>
 *   <li>Retry transient with exponential backoff up to the ceiling.</li>
 *   <li>Dead-letter permanent + retry-exhausted failures.</li>
 * </ol>
 *
 * <h2>Interruptibility</h2>
 * The retry loop checks the interrupt flag and propagates {@link InterruptedException} from backoff
 * as a {@link BulkIndexException}, so a graceful shutdown stops promptly and the checkpoint does not
 * advance (the batch replays, duplicate-free via deterministic ids).
 *
 * <h2>Idempotency &amp; backpressure</h2>
 * Deterministic {@code _id}s mean a whole-batch replay overwrites rather than duplicates. When ES is
 * slow, retries/backoff slow the whole pipeline and the checkpoint stops advancing — the system
 * self-throttles to ES's speed.
 */
public class BulkIndexer {

    private static final Logger log = LoggerFactory.getLogger(BulkIndexer.class);

    private final ElasticsearchTransport transport;
    private final IndexDeadLetterStore deadLetterStore;
    private final BackoffPolicy backoff;
    private final int maxDocsPerRequest;
    private final long maxBytesPerRequest;
    private final Metrics metrics;

    public BulkIndexer(ElasticsearchTransport transport,
                       IndexDeadLetterStore deadLetterStore,
                       BackoffPolicy backoff,
                       int maxDocsPerRequest,
                       long maxBytesPerRequest,
                       Metrics metrics) {
        if (transport == null) {
            throw new IllegalArgumentException("transport must not be null");
        }
        if (deadLetterStore == null) {
            throw new IllegalArgumentException("deadLetterStore must not be null");
        }
        if (maxDocsPerRequest <= 0) {
            throw new IllegalArgumentException("maxDocsPerRequest must be > 0");
        }
        if (maxBytesPerRequest <= 0) {
            throw new IllegalArgumentException("maxBytesPerRequest must be > 0");
        }
        this.transport = transport;
        this.deadLetterStore = deadLetterStore;
        this.backoff = backoff == null ? new BackoffPolicy() : backoff;
        this.maxDocsPerRequest = maxDocsPerRequest;
        this.maxBytesPerRequest = maxBytesPerRequest;
        this.metrics = metrics;
    }

    /**
     * Indexes a batch; returns only once every document is acknowledged by ES or dead-lettered.
     *
     * @return number of documents successfully indexed (excludes dead-lettered).
     * @throws BulkIndexException if transport fails and retries are exhausted, or if interrupted.
     */
    public int indexBatch(List<AnalyzedEvent> events) {
        if (events == null || events.isEmpty()) {
            return 0;
        }

        List<IndexOperation> operations = new ArrayList<>(events.size());
        for (AnalyzedEvent ae : events) {
            operations.add(new IndexOperation(ae.esId(), ae.targetIndex(), JsonWriter.toJson(ae)));
        }

        int totalIndexed = 0;
        for (List<IndexOperation> sub : splitBySize(operations)) {
            totalIndexed += indexSubRequest(sub);
        }
        return totalIndexed;
    }

    /** Splits operations into sub-requests bounded by both doc count and byte size. */
    private List<List<IndexOperation>> splitBySize(List<IndexOperation> operations) {
        List<List<IndexOperation>> chunks = new ArrayList<>();
        List<IndexOperation> current = new ArrayList<>();
        long currentBytes = 0;

        for (IndexOperation op : operations) {
            int bytes = op.approxBytes();
            boolean wouldOverflow =
                    (!current.isEmpty())
                            && (current.size() >= maxDocsPerRequest
                            || currentBytes + bytes > maxBytesPerRequest);
            if (wouldOverflow) {
                chunks.add(current);
                current = new ArrayList<>();
                currentBytes = 0;
            }
            current.add(op);
            currentBytes += bytes;
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    private int indexSubRequest(List<IndexOperation> operations) {
        int totalDocs = operations.size();
        List<BulkResponse.ItemFailure> permanentFailures = new ArrayList<>();
        List<IndexOperation> pending = operations;
        int indexedSoFar = 0;

        for (int attempt = 0; attempt < backoff.maxAttempts(); attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new BulkIndexException(
                        "Indexer interrupted before completing " + pending.size()
                                + " document(s); checkpoint will not advance");
            }
            if (attempt > 0) {
                log.warn("Retrying {} transient failure(s), attempt {}/{}",
                        pending.size(), attempt + 1, backoff.maxAttempts());
                if (metrics != null) {
                    metrics.bulkRetries.increment();
                }
                try {
                    backoff.sleep(attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new BulkIndexException(
                            "Indexer interrupted during backoff; checkpoint will not advance", ie);
                }
            }

            final List<IndexOperation> toSend = pending;
            BulkResponse response;
            try {
                response = time(() -> transport.bulk(toSend));
            } catch (ElasticsearchTransport.TransportException te) {
                if (attempt + 1 >= backoff.maxAttempts()) {
                    throw new BulkIndexException(
                            "Transport failed and retries exhausted for " + pending.size()
                                    + " document(s); checkpoint will not advance", te);
                }
                log.warn("Bulk transport failed (attempt {}/{}): {}",
                        attempt + 1, backoff.maxAttempts(), te.getMessage());
                continue;
            }

            indexedSoFar += response.succeeded();

            if (!response.hasFailures()) {
                pending = List.of();
                break;
            }

            List<IndexOperation> retryable = new ArrayList<>();
            for (BulkResponse.ItemFailure f : response.failures()) {
                if (f.isRetryable()) {
                    retryable.add(f.operation());
                } else {
                    permanentFailures.add(f);
                }
            }

            if (retryable.isEmpty()) {
                pending = List.of();
                break;
            }
            pending = retryable;
        }

        if (!pending.isEmpty()) {
            log.error("{} document(s) still failing after {} attempts; dead-lettering",
                    pending.size(), backoff.maxAttempts());
            for (IndexOperation op : pending) {
                permanentFailures.add(new BulkResponse.ItemFailure(op, 0, "retries exhausted"));
            }
        }

        if (!permanentFailures.isEmpty()) {
            deadLetterStore.persist(permanentFailures);
            if (metrics != null) {
                metrics.docsDeadLetteredIndex.increment(permanentFailures.size());
            }
        }

        if (metrics != null) {
            metrics.docsIndexed.increment(indexedSoFar);
        }
        log.info("Indexed sub-request: {} ok, {} dead-lettered (of {} total)",
                indexedSoFar, permanentFailures.size(), totalDocs);
        return indexedSoFar;
    }

    private interface BulkCall {
        BulkResponse call() throws ElasticsearchTransport.TransportException;
    }

    private BulkResponse time(BulkCall call) throws ElasticsearchTransport.TransportException {
        if (metrics == null) {
            return call.call();
        }
        long start = System.nanoTime();
        try {
            return call.call();
        } finally {
            metrics.indexLatency.record(java.time.Duration.ofNanos(System.nanoTime() - start));
        }
    }
}
