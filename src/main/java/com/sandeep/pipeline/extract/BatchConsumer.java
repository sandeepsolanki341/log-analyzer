package com.sandeep.pipeline.extract;

import java.util.List;

/**
 * Downstream sink for a single batch of extracted records.
 *
 * <p><strong>Contract:</strong> the consumer must process the batch <em>synchronously</em> and
 * either return normally (success; the extractor then advances the checkpoint) or throw (the
 * extractor will not advance, so the batch is retried next run). The consumer must therefore make
 * its writes idempotent (deterministic document ids in the sink).
 */
@FunctionalInterface
public interface BatchConsumer {

    /**
     * Process one batch of raw records. Must throw if processing fails.
     *
     * @param batch a non-empty, id-ordered list of records.
     * @throws Exception if the batch could not be fully processed; prevents checkpoint advance.
     */
    void accept(List<RawLogRecord> batch) throws Exception;
}
