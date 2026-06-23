package com.sandeep.pipeline.index;

/**
 * One unit of work in a bulk request: index this {@code json} document under {@code id} into
 * {@code index}. Built from an {@code AnalyzedEvent} (which already decided the id and index).
 *
 * @param id    deterministic Elasticsearch {@code _id} (idempotency key).
 * @param index target time-based index name.
 * @param json  serialized document body.
 */
public record IndexOperation(String id, String index, String json) {

    /** Approximate wire size of this operation's body in bytes (UTF-8), for byte-bounded batching. */
    public int approxBytes() {
        return json == null ? 0 : json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }
}
