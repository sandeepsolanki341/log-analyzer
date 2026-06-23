package com.sandeep.pipeline.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Single shared, thread-safe Jackson {@link ObjectMapper}.
 *
 * <p>{@code ObjectMapper} is thread-safe once configured, so one instance is shared process-wide
 * rather than allocated per call. JSR-310 is registered so {@code Instant} serializes as ISO-8601
 * (Elasticsearch's native {@code date} format) instead of an epoch array.
 */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private Json() {
    }
}
