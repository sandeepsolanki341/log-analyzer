package com.sandeep.pipeline.analyze;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Optional;

/**
 * Redis-backed {@link BaselineStore}. The trailing EMA baseline survives restarts so anomaly
 * detection resumes from where it left off instead of cold-starting.
 *
 * <h2>Failure policy</h2>
 * Redis is treated as best-effort for the baseline: a transient Redis outage must NOT take the whole
 * ingestion pipeline down. On a read error we return empty (detector falls back to seeding); on a
 * write error we log and continue. The cost of a missed baseline update is a slightly stale EMA, not
 * data loss in the log stream itself.
 */
public class RedisBaselineStore implements BaselineStore {

    private static final Logger log = LoggerFactory.getLogger(RedisBaselineStore.class);

    private final JedisPool pool;
    private final String keyPrefix;

    public RedisBaselineStore(JedisPool pool, String keyPrefix) {
        if (pool == null) {
            throw new IllegalArgumentException("pool must not be null");
        }
        this.pool = pool;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }

    @Override
    public Optional<Double> get(String key) {
        try (Jedis jedis = pool.getResource()) {
            String v = jedis.get(keyPrefix + key);
            if (v == null) {
                return Optional.empty();
            }
            return Optional.of(Double.parseDouble(v));
        } catch (JedisException | NumberFormatException e) {
            log.warn("Baseline read from Redis failed for key '{}' (continuing without): {}",
                    key, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public void set(String key, double value) {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(keyPrefix + key, Double.toString(value));
        } catch (JedisException e) {
            log.warn("Baseline write to Redis failed for key '{}' (continuing): {}",
                    key, e.getMessage());
        }
    }
}
