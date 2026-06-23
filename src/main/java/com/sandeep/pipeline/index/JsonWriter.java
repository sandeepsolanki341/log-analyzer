package com.sandeep.pipeline.index;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sandeep.pipeline.analyze.AnalyzedEvent;
import com.sandeep.pipeline.parse.LogEvent;
import com.sandeep.pipeline.util.Json;

import java.util.Map;

/**
 * Serializes an {@link AnalyzedEvent} into the flat JSON document body stored in Elasticsearch.
 *
 * <p>Backed by the shared Jackson mapper so escaping, unicode, control characters, and large bodies
 * are handled correctly (the previous hand-rolled writer was fine for the common case but a real
 * dependency removes a class of edge-case bugs). The emitted field names are the contract with the
 * index mapping — keep them in sync. Timestamps are ISO-8601 UTC.
 */
public final class JsonWriter {

    private JsonWriter() {
    }

    public static String toJson(AnalyzedEvent ae) {
        LogEvent e = ae.event();
        ObjectNode root = Json.MAPPER.createObjectNode();

        root.put("@timestamp", e.timestamp().toString());
        root.put("level", e.level().name());
        root.put("service", e.service());
        root.put("message", e.message());
        root.put("sourceId", e.sourceId());
        if (e.userId() != null) {
            root.put("userId", e.userId());
        } else {
            root.putNull("userId");
        }
        root.put("ip", e.ip());
        if (e.statusCode() != null) {
            root.put("statusCode", e.statusCode());
        } else {
            root.putNull("statusCode");
        }
        if (e.latencyMs() != null) {
            root.put("latencyMs", e.latencyMs());
        } else {
            root.putNull("latencyMs");
        }
        root.put("stackTrace", e.stackTrace());

        root.put("category", ae.classification().category());
        root.put("domain", ae.classification().domain());
        root.put("severity", ae.classification().severity().name());
        root.put("fingerprint", ae.fingerprint());
        root.put("traceId", ae.traceId());
        root.put("synthetic", ae.synthetic());

        ObjectNode enrichment = root.putObject("enrichment");
        for (Map.Entry<String, String> en : ae.enrichment().entrySet()) {
            enrichment.put(en.getKey(), en.getValue());
        }
        ObjectNode fields = root.putObject("fields");
        for (Map.Entry<String, String> en : e.extractedFields().entrySet()) {
            fields.put(en.getKey(), en.getValue());
        }

        return root.toString();
    }
}
