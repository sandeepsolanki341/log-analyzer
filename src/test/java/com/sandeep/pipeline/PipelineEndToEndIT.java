package com.sandeep.pipeline;

import com.sandeep.pipeline.config.PipelineConfig;
import com.sandeep.pipeline.runner.PipelineRunner;
import com.sandeep.pipeline.runner.PipelineWiring;
import com.sandeep.pipeline.util.Metrics;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Full pipeline integration test against real MySQL and Elasticsearch (via Testcontainers). Seeds
 * rows, runs the pipeline, and asserts documents land in Elasticsearch with the deterministic id.
 * Redis is omitted here — the wiring falls back to the in-memory baseline store when Redis is absent,
 * which this test exercises. Named {@code *IT} so it runs in the failsafe (verify) phase, not unit
 * tests. Requires Docker.
 */
@Testcontainers
class PipelineEndToEndIT {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("logs")
            .withUsername("test")
            .withPassword("test");

    @Container
    static final ElasticsearchContainer ES = new ElasticsearchContainer(
            "docker.elastic.co/elasticsearch/elasticsearch:8.15.0")
            .withEnv("xpack.security.enabled", "false")
            .withEnv("discovery.type", "single-node");

    @Test
    void seededRowsAreIndexedIntoElasticsearch() throws Exception {
        initSchemaAndSeed();

        Properties props = new Properties();
        String jdbc = MYSQL.getJdbcUrl()
                + "?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true";
        props.setProperty("db.url", jdbc);
        props.setProperty("db.user", MYSQL.getUsername());
        props.setProperty("db.password", MYSQL.getPassword());
        props.setProperty("es.host", ES.getHost());
        props.setProperty("es.port", String.valueOf(ES.getMappedPort(9200)));
        props.setProperty("es.scheme", "http");
        props.setProperty("redis.host", "localhost"); // absent -> in-memory fallback
        props.setProperty("run.interval.seconds", "2");
        props.setProperty("batch.size", "1000");
        props.setProperty("health.port", "0"); // not started in this test

        PipelineConfig cfg = PipelineConfig.load(props);
        Metrics metrics = new Metrics();
        try (PipelineWiring wiring = PipelineWiring.build(cfg, metrics)) {
            PipelineRunner runner = new PipelineRunner(wiring, cfg, metrics);
            runner.start();

            HttpClient http = HttpClient.newHttpClient();
            String esBase = "http://" + ES.getHost() + ":" + ES.getMappedPort(9200);

            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
                HttpResponse<String> resp = http.send(
                        HttpRequest.newBuilder(URI.create(esBase + "/app-logs-*/_refresh")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                HttpResponse<String> count = http.send(
                        HttpRequest.newBuilder(URI.create(esBase + "/app-logs-*/_count")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertTrue(count.body().contains("\"count\""));
                // crude parse: count should be >= 3 seeded log rows
                int c = Integer.parseInt(count.body().replaceAll(".*\"count\":(\\d+).*", "$1"));
                assertTrue(c >= 3, "expected >=3 docs, body=" + count.body());
            });

            runner.stop();
        }
    }

    private void initSchemaAndSeed() throws Exception {
        try (Connection c = DriverManager.getConnection(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
             Statement st = c.createStatement()) {
            st.execute("CREATE TABLE app_logs (id BIGINT AUTO_INCREMENT PRIMARY KEY, ts DATETIME(3) NOT NULL,"
                    + "level VARCHAR(16), service VARCHAR(128), message TEXT, user_id BIGINT, ip VARCHAR(64),"
                    + "status_code INT, latency_ms BIGINT, stack_trace MEDIUMTEXT)");
            st.execute("CREATE TABLE pipeline_state (pipeline_name VARCHAR(128) PRIMARY KEY,"
                    + "last_processed_id BIGINT NOT NULL DEFAULT 0)");
            st.execute("CREATE TABLE parse_dead_letter (source_id BIGINT PRIMARY KEY, raw_message TEXT,"
                    + "reason VARCHAR(512) NOT NULL, failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            st.execute("CREATE TABLE index_dead_letter (es_id VARCHAR(512) PRIMARY KEY, target_index VARCHAR(255),"
                    + "http_status INT, reason VARCHAR(1024), body MEDIUMTEXT, failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            st.execute("INSERT INTO app_logs (ts, level, service, message, status_code) VALUES "
                    + "(UTC_TIMESTAMP(),'INFO','checkout','order ok',200),"
                    + "(UTC_TIMESTAMP(),'ERROR','checkout','boom',503),"
                    + "(UTC_TIMESTAMP(),'WARN','auth','bad creds',401)");
        }
    }
}
