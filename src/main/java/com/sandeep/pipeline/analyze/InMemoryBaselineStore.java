package com.sandeep.pipeline.analyze;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Non-durable {@link BaselineStore} for tests and for running without Redis. State is lost on
 * restart (the warm-up behavior the durable store exists to avoid).
 */
public class InMemoryBaselineStore implements BaselineStore {

    private final Map<String, Double> map = new ConcurrentHashMap<>();

    @Override
    public Optional<Double> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void set(String key, double value) {
        map.put(key, value);
    }
}
