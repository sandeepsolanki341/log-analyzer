package com.sandeep.pipeline.health;

import com.sandeep.pipeline.util.Metrics;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

/**
 * Tiny dependency-free HTTP server exposing operational endpoints, backed by the JDK's built-in
 * {@code com.sun.net.httpserver}:
 *
 * <ul>
 *   <li>{@code GET /health/live} — liveness: always 200 while the process is up.</li>
 *   <li>{@code GET /health/ready} — readiness: 200 only when dependencies (DB/ES/Redis) are wired
 *       and a recent run has completed; 503 otherwise. Drives Kubernetes readiness gating.</li>
 *   <li>{@code GET /metrics} — Prometheus exposition for scraping.</li>
 * </ul>
 *
 * <p>Kept intentionally minimal: a real service may front this with a fuller framework, but for a
 * single-purpose ETL worker this avoids dragging in a web stack just for health checks.
 */
public class HealthServer {

    private static final Logger log = LoggerFactory.getLogger(HealthServer.class);

    private final HttpServer server;
    private final Metrics metrics;
    private final BooleanSupplier readiness;

    public HealthServer(int port, Metrics metrics, BooleanSupplier readiness) throws IOException {
        this.metrics = metrics;
        this.readiness = readiness;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "health-http");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/health/live", this::handleLive);
        server.createContext("/health/ready", this::handleReady);
        server.createContext("/metrics", this::handleMetrics);
    }

    public void start() {
        server.start();
        log.info("Health server listening on {}", server.getAddress());
    }

    public void stop() {
        server.stop(0);
    }

    private void handleLive(HttpExchange ex) throws IOException {
        respond(ex, 200, "OK");
    }

    private void handleReady(HttpExchange ex) throws IOException {
        boolean ready = readiness.getAsBoolean();
        respond(ex, ready ? 200 : 503, ready ? "READY" : "NOT_READY");
    }

    private void handleMetrics(HttpExchange ex) throws IOException {
        respond(ex, 200, metrics.scrape());
    }

    private void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
