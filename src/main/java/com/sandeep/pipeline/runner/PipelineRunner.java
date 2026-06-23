package com.sandeep.pipeline.runner;

import com.sandeep.pipeline.config.PipelineConfig;
import com.sandeep.pipeline.extract.LogExtractor;
import com.sandeep.pipeline.util.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives the pipeline on a schedule with three production-critical guarantees:
 *
 * <ol>
 *   <li><b>Non-overlapping runs.</b> A single-thread scheduler with {@code scheduleWithFixedDelay}
 *       means the next run starts only after the previous one finishes (plus the delay). If a run
 *       blocks for minutes on Elasticsearch backoff, the scheduler will not fire an overlapping run
 *       — which would violate the single-writer assumption of the aggregator and checkpoint.</li>
 *   <li><b>Crash isolation per tick.</b> An exception in one run is logged and counted, but does not
 *       kill the scheduler; the next tick retries from the durable checkpoint.</li>
 *   <li><b>Graceful shutdown.</b> On stop, the in-flight run is interrupted, and we wait up to the
 *       configured grace period for it to unwind cleanly (so an in-progress batch either finishes or
 *       leaves the checkpoint un-advanced for a clean replay).</li>
 * </ol>
 */
public class PipelineRunner {

    private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);

    private final LogExtractor extractor;
    private final PipelineChain chain;
    private final Metrics metrics;
    private final Duration interval;
    private final Duration shutdownGrace;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean ranAtLeastOnceSuccessfully = false;

    public PipelineRunner(PipelineWiring wiring, PipelineConfig cfg, Metrics metrics) {
        this.extractor = wiring.extractor();
        this.chain = wiring.chain();
        this.metrics = metrics;
        this.interval = cfg.runInterval;
        this.shutdownGrace = cfg.shutdownGrace;
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "pipeline-runner");
            t.setDaemon(false);
            return t;
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(tf);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long delaySec = Math.max(1, interval.toSeconds());
        scheduler.scheduleWithFixedDelay(this::runTickSafely, 0, delaySec, TimeUnit.SECONDS);
        log.info("Pipeline scheduler started (fixed delay {}s, non-overlapping)", delaySec);
    }

    /** One scheduled tick. Never throws (would silently kill the scheduler). */
    private void runTickSafely() {
        if (!running.get()) {
            return;
        }
        long start = System.nanoTime();
        try {
            long processed = extractor.runOnce(chain, metrics::setBacklog);
            metrics.markRun();
            ranAtLeastOnceSuccessfully = true;
            if (processed > 0) {
                log.info("Tick complete: {} record(s) processed", processed);
            }
        } catch (Throwable t) {
            metrics.runFailures.increment();
            log.error("Pipeline tick failed (will retry next interval from durable checkpoint)", t);
        } finally {
            metrics.runLatency.record(Duration.ofNanos(System.nanoTime() - start));
        }
    }

    /** True once at least one run has completed (used for readiness gating). */
    public boolean ready() {
        return ranAtLeastOnceSuccessfully;
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping pipeline scheduler (grace {}s)...", shutdownGrace.toSeconds());
        scheduler.shutdown(); // stop scheduling new ticks
        try {
            // Interrupt the in-flight run so blocking backoff/IO unwinds promptly.
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!scheduler.awaitTermination(shutdownGrace.toSeconds(), TimeUnit.SECONDS)) {
                log.warn("Scheduler did not terminate within grace period");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        log.info("Pipeline scheduler stopped.");
    }
}
