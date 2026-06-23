package com.sandeep.pipeline.extract;

/**
 * Unchecked exception thrown when the extraction layer cannot safely continue. Wraps the root
 * {@link java.sql.SQLException} as cause. When this propagates out of the extractor, the checkpoint
 * has <strong>not</strong> advanced for the failed batch, so the next run safely re-reads the same
 * window.
 */
public class PipelineExtractionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PipelineExtractionException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineExtractionException(String message) {
        super(message);
    }
}
