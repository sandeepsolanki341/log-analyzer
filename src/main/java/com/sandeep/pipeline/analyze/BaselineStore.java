package com.sandeep.pipeline.analyze;

import java.util.Optional;

/**
 * Durable store for the anomaly detector's trailing baseline (the EMA of the error rate). Persisting
 * it means a pipeline restart does not reset anomaly detection to a cold "warm-up" state and either
 * miss a spike that was already elevated or fire spuriously while re-learning.
 *
 * <p>Implementations must be safe for the single pipeline writer; the read-modify-write is performed
 * by the detector, so the store only needs simple get/set semantics.
 */
public interface BaselineStore {

    /** @return the persisted EMA for {@code key}, or empty if none stored yet. */
    Optional<Double> get(String key);

    /** Persists {@code value} for {@code key}. */
    void set(String key, double value);
}
