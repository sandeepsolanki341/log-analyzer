package com.sandeep.pipeline.runner;

import com.sandeep.pipeline.analyze.AnalyzedEvent;
import com.sandeep.pipeline.analyze.Analyzer;
import com.sandeep.pipeline.extract.BatchConsumer;
import com.sandeep.pipeline.extract.RawLogRecord;
import com.sandeep.pipeline.index.BulkIndexer;
import com.sandeep.pipeline.parse.DeadLetterStore;
import com.sandeep.pipeline.parse.LogParser;
import com.sandeep.pipeline.parse.ParseResult;
import com.sandeep.pipeline.util.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Wires the parse → analyze → index stages into a single {@link BatchConsumer} the extractor can
 * drive. The synchronous handoff here is deliberate: it is the pipeline's natural backpressure. If
 * Elasticsearch slows, {@link BulkIndexer} blocks here, which blocks the extractor from fetching
 * more rows — the system self-throttles instead of piling rows into memory.
 *
 * <h2>Failure contract</h2>
 * Parsing quarantines bad rows (never throws for one poison row). Indexing either fully accounts for
 * every document (indexed or dead-lettered) and returns, or throws {@code BulkIndexException} when ES
 * is unreachable and retries are exhausted. A throw here propagates up through the extractor, which
 * does NOT advance the checkpoint, so the batch replays next tick — duplicate-free via deterministic
 * ids.
 */
public class PipelineChain implements BatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(PipelineChain.class);

    private final LogParser parser;
    private final DeadLetterStore parseDeadLetters;
    private final Analyzer analyzer;
    private final BulkIndexer indexer;
    private final Metrics metrics;

    public PipelineChain(LogParser parser, DeadLetterStore parseDeadLetters,
                         Analyzer analyzer, BulkIndexer indexer, Metrics metrics) {
        this.parser = parser;
        this.parseDeadLetters = parseDeadLetters;
        this.analyzer = analyzer;
        this.indexer = indexer;
        this.metrics = metrics;
    }

    @Override
    public void accept(List<RawLogRecord> batch) throws Exception {
        metrics.rowsExtracted.increment(batch.size());

        // 1. Parse (poison-pill isolation)
        ParseResult parsed = parser.parseBatch(batch);
        metrics.rowsParsed.increment(parsed.successCount());
        if (parsed.hasFailures()) {
            parseDeadLetters.persist(parsed.deadLetters());
            metrics.rowsDeadLetteredParse.increment(parsed.failureCount());
        }

        if (parsed.events().isEmpty()) {
            log.debug("Batch produced no parseable events ({} quarantined)", parsed.failureCount());
            return;
        }

        // 2. Analyze (enrich + windowed anomaly detection)
        List<AnalyzedEvent> analyzed = analyzer.analyzeBatch(parsed.events());
        metrics.eventsAnalyzed.increment(parsed.events().size());
        long alertCount = analyzed.stream().filter(AnalyzedEvent::synthetic).count();
        if (alertCount > 0) {
            metrics.alertsRaised.increment(alertCount);
        }

        // 3. Index (idempotent, sub-batched, backpressure point). Throws on unrecoverable ES failure.
        indexer.indexBatch(analyzed);
    }
}
