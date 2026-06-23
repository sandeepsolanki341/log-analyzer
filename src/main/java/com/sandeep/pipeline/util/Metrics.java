package com.sandeep.pipeline.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Central Micrometer registry plus the handful of pipeline meters worth tracking in production.
 *
 * <p>Exposes a Prometheus scrape endpoint via {@link #scrape()} (wired into the health server). All
 * meters are created once and reused; counters/timers are cheap to increment from the single
 * pipeline thread.
 */
public final class Metrics {

    private final PrometheusMeterRegistry registry =
            new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    // Extraction
    public final Counter rowsExtracted;
    // Parsing
    public final Counter rowsParsed;
    public final Counter rowsDeadLetteredParse;
    // Analysis
    public final Counter eventsAnalyzed;
    public final Counter alertsRaised;
    // Indexing
    public final Counter docsIndexed;
    public final Counter docsDeadLetteredIndex;
    public final Counter bulkRetries;
    public final Timer indexLatency;
    public final Timer runLatency;
    public final Counter runFailures;

    private final AtomicLong lastRunEpochSeconds = new AtomicLong(0);
    private final AtomicLong backlogGauge = new AtomicLong(0);

    public Metrics() {
        this.rowsExtracted = Counter.builder("pipeline.rows.extracted").register(registry);
        this.rowsParsed = Counter.builder("pipeline.rows.parsed").register(registry);
        this.rowsDeadLetteredParse = Counter.builder("pipeline.deadletter.parse").register(registry);
        this.eventsAnalyzed = Counter.builder("pipeline.events.analyzed").register(registry);
        this.alertsRaised = Counter.builder("pipeline.alerts.raised").register(registry);
        this.docsIndexed = Counter.builder("pipeline.docs.indexed").register(registry);
        this.docsDeadLetteredIndex = Counter.builder("pipeline.deadletter.index").register(registry);
        this.bulkRetries = Counter.builder("pipeline.bulk.retries").register(registry);
        this.indexLatency = Timer.builder("pipeline.index.latency").register(registry);
        this.runLatency = Timer.builder("pipeline.run.latency").register(registry);
        this.runFailures = Counter.builder("pipeline.run.failures").register(registry);

        Gauge.builder("pipeline.last.run.epoch.seconds", lastRunEpochSeconds, AtomicLong::get)
                .register(registry);
        Gauge.builder("pipeline.backlog.estimate", backlogGauge, AtomicLong::get)
                .register(registry);
    }

    public void markRun() {
        lastRunEpochSeconds.set(System.currentTimeMillis() / 1000);
    }

    public void setBacklog(long n) {
        backlogGauge.set(n);
    }

    public long lastRunEpochSeconds() {
        return lastRunEpochSeconds.get();
    }

    public MeterRegistry registry() {
        return registry;
    }

    public String scrape() {
        return registry.scrape();
    }
}
