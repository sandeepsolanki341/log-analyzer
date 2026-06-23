package com.sandeep.pipeline.runner;

import com.sandeep.pipeline.config.PipelineConfig;
import com.sandeep.pipeline.health.HealthServer;
import com.sandeep.pipeline.util.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point. Loads config, builds the object graph, starts the health server and the scheduler,
 * and installs a shutdown hook for graceful termination on SIGTERM/SIGINT (the signals Kubernetes
 * and Docker send on stop).
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Properties props = loadProperties();
        PipelineConfig cfg = PipelineConfig.load(props);
        Metrics metrics = new Metrics();

        log.info("Starting HealthSpan log-analysis pipeline '{}' (batchSize={}, window={}s, interval={}s)",
                cfg.pipelineName, cfg.batchSize, cfg.window.toSeconds(), cfg.runInterval.toSeconds());

        PipelineWiring wiring = PipelineWiring.build(cfg, metrics);
        PipelineRunner runner = new PipelineRunner(wiring, cfg, metrics);

        HealthServer health = new HealthServer(cfg.healthPort, metrics,
                () -> runner.ready() && wiring.dbReachable());
        health.start();

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received.");
            try {
                runner.stop();
            } finally {
                health.stop();
                wiring.close();
                shutdownLatch.countDown();
            }
        }, "shutdown-hook"));

        runner.start();
        log.info("Pipeline is up. Health on :{} (/health/live, /health/ready, /metrics)", cfg.healthPort);

        shutdownLatch.await(); // block main thread until shutdown completes
        log.info("Pipeline exited cleanly.");
    }

    /** Loads application.properties from the classpath if present (env vars still take priority). */
    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream in = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
                log.info("Loaded application.properties from classpath");
            }
        } catch (Exception e) {
            log.warn("Could not load application.properties: {}", e.getMessage());
        }
        return props;
    }

    private Main() {
    }
}
