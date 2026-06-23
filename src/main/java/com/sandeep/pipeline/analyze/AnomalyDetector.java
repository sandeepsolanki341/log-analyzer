package com.sandeep.pipeline.analyze;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Stage 4 — Anomaly detection. Applies static and relative rules against a {@link WindowSnapshot},
 * raising {@link Alert}s when thresholds are crossed.
 *
 * <h2>Durable trailing baseline</h2>
 * The relative rule compares the current error rate against an exponential moving average baseline.
 * That EMA is persisted through a {@link BaselineStore} (Redis in production), so a restart resumes
 * from the learned baseline instead of cold-starting — no warm-up window during which spikes are
 * missed or invented. The read-modify-write happens per evaluation; the store is best-effort and
 * never blocks ingestion.
 *
 * <p>Alerts are stamped with the window's event-time {@code now} (the aggregator watermark), so on
 * the analyzer side they get a deterministic, replay-stable id and land in the correct time-based
 * index.
 *
 * <p>Not thread-safe for concurrent {@code evaluate}; driven from the single pipeline thread.
 */
public class AnomalyDetector {

    private static final String EMA_KEY = "error-rate-ema";

    private final double errorRateSpikeMultiplier;
    private final long minEventsForRateRule;
    private final long failedLoginThreshold;
    private final double emaAlpha;
    private final BaselineStore baselineStore;

    public AnomalyDetector(BaselineStore baselineStore) {
        this(3.0, 50, 20, 0.3, baselineStore);
    }

    public AnomalyDetector(double errorRateSpikeMultiplier, long minEventsForRateRule,
                           long failedLoginThreshold, double emaAlpha, BaselineStore baselineStore) {
        if (baselineStore == null) {
            throw new IllegalArgumentException("baselineStore must not be null");
        }
        this.errorRateSpikeMultiplier = errorRateSpikeMultiplier;
        this.minEventsForRateRule = minEventsForRateRule;
        this.failedLoginThreshold = failedLoginThreshold;
        this.emaAlpha = emaAlpha;
        this.baselineStore = baselineStore;
    }

    public List<Alert> evaluate(WindowSnapshot snapshot, Instant now) {
        List<Alert> alerts = new ArrayList<>();

        // --- Relative rule: error-rate spike vs durable trailing baseline ---
        double currentRate = snapshot.errorRate();
        if (snapshot.totalEvents() >= minEventsForRateRule) {
            Double baseline = baselineStore.get(EMA_KEY).orElse(null);
            if (baseline != null && baseline > 0
                    && currentRate > errorRateSpikeMultiplier * baseline) {
                alerts.add(new Alert(now, "ERROR_RATE_SPIKE", "GLOBAL",
                        String.format("Error rate %.1f%% exceeds %.1f x trailing avg %.1f%% over %ds",
                                currentRate * 100, errorRateSpikeMultiplier,
                                baseline * 100, snapshot.windowSeconds()),
                        SeverityBucket.CRITICAL));
            }
            updateBaseline(baseline, currentRate);
        }

        // --- Static rule: per-service error concentration ---
        snapshot.errorsByService().forEach((service, count) -> {
            if (count >= Math.max(10, minEventsForRateRule / 2)) {
                alerts.add(new Alert(now, "SERVICE_ERROR_CONCENTRATION", service,
                        String.format("Service '%s' produced %d errors in %ds",
                                service, count, snapshot.windowSeconds()),
                        SeverityBucket.CRITICAL));
            }
        });

        // --- Static rule: failed-login burst per IP ---
        snapshot.failedLoginsByIp().forEach((ip, count) -> {
            if (count >= failedLoginThreshold) {
                alerts.add(new Alert(now, "FAILED_LOGIN_BURST", ip,
                        String.format("%d failed auth attempts from %s in %ds",
                                count, ip, snapshot.windowSeconds()),
                        SeverityBucket.DEGRADED));
            }
        });

        return alerts;
    }

    private void updateBaseline(Double existing, double currentRate) {
        double updated = (existing == null)
                ? currentRate
                : emaAlpha * currentRate + (1 - emaAlpha) * existing;
        baselineStore.set(EMA_KEY, updated);
    }
}
