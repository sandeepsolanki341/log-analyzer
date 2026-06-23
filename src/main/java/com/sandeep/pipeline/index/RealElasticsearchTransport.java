package com.sandeep.pipeline.index;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Live {@link com.sandeep.pipeline.index.ElasticsearchTransport} backed by the official
 * {@code co.elastic.clients} client (8.15). Supports basic-auth or API-key auth and sends the
 * pre-serialized {@link JsonWriter} body verbatim via {@link BinaryData} (no re-mapping), under a
 * deterministic {@code _id} so retries overwrite rather than duplicate.
 *
 * <p>Per-item status codes drive {@link BulkResponse.ItemFailure#isRetryable()}: 429/5xx retry,
 * other 4xx (mapping/parse) dead-letter. Whole-request failures (connection refused, socket timeout)
 * surface as {@link TransportException}.
 */
public class RealElasticsearchTransport implements com.sandeep.pipeline.index.ElasticsearchTransport {

    private static final Logger log = LoggerFactory.getLogger(RealElasticsearchTransport.class);

    private final RestClient restClient;
    private final RestClientTransport transport;
    private final ElasticsearchClient client;

    public RealElasticsearchTransport(String host, int port, String scheme,
                                      String username, String password, String apiKey) {
        this.restClient = buildRestClient(host, port, scheme, username, password, apiKey);
        this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
        log.info("Elasticsearch transport initialized at {}://{}:{}", scheme, host, port);
    }

    private static RestClient buildRestClient(String host, int port, String scheme,
                                              String username, String password, String apiKey) {
        var builder = RestClient.builder(new HttpHost(host, port, scheme));
        if (apiKey != null && !apiKey.isBlank()) {
            Header[] headers = {new BasicHeader("Authorization", "ApiKey " + apiKey)};
            builder.setDefaultHeaders(headers);
        } else if (username != null && !username.isBlank()) {
            CredentialsProvider creds = new BasicCredentialsProvider();
            creds.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password == null ? "" : password));
            builder.setHttpClientConfigCallback(cb -> cb.setDefaultCredentialsProvider(creds));
        }
        return builder.build();
    }

    @Override
    public BulkResponse bulk(List<IndexOperation> operations) throws TransportException {
        try {
            BulkRequest.Builder br = new BulkRequest.Builder();
            for (IndexOperation op : operations) {
                BinaryData body = BinaryData.of(
                        op.json().getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
                br.operations(o -> o.index(idx -> idx
                        .index(op.index())
                        .id(op.id())
                        .document(body)));
            }

            co.elastic.clients.elasticsearch.core.BulkResponse resp = client.bulk(br.build());

            List<BulkResponse.ItemFailure> failures = new ArrayList<>();
            int i = 0;
            for (BulkResponseItem item : resp.items()) {
                if (item.error() != null) {
                    int status = item.status();
                    String reason = item.error().reason();
                    failures.add(new BulkResponse.ItemFailure(operations.get(i), status, reason));
                }
                i++;
            }
            return new BulkResponse(operations.size(), failures);

        } catch (IOException e) {
            throw new TransportException("bulk request failed (transport-level)", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            transport.close();
        } finally {
            restClient.close();
        }
    }
}
