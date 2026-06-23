package com.sandeep.pipeline.config;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Properties;
import java.util.function.Function;

/**
 * Immutable, validated configuration for the whole pipeline, resolved from (in priority order):
 * environment variables, then a {@link Properties} object (e.g. loaded from {@code application.properties}),
 * then built-in defaults.
 *
 * <p>Env keys are upper snake case (e.g. {@code DB_URL}); property keys are dotted lower case
 * (e.g. {@code db.url}). The two are interchangeable via {@link #normalize}.
 */
public final class PipelineConfig {

    // --- database ---
    public final String dbUrl;
    public final String dbUser;
    public final String dbPassword;
    public final int dbPoolSize;
    public final ZoneId sourceZoneForNaiveTimestamps;

    // --- extraction ---
    public final String pipelineName;
    public final int batchSize;

    // --- indexing ---
    public final String esHost;
    public final int esPort;
    public final String esScheme;
    public final String esUsername;
    public final String esPassword;
    public final String esApiKey;
    public final int indexSubBatchSize;
    public final long indexMaxBytesPerRequest;
    public final int bulkMaxAttempts;
    public final long bulkBaseDelayMs;
    public final long bulkMaxDelayMs;

    // --- analysis ---
    public final Duration window;
    public final double errorRateSpikeMultiplier;
    public final long minEventsForRateRule;
    public final long failedLoginThreshold;
    public final double emaAlpha;

    // --- redis (anomaly baseline) ---
    public final String redisHost;
    public final int redisPort;
    public final String redisPassword;
    public final String redisKeyPrefix;

    // --- scheduling / ops ---
    public final Duration runInterval;
    public final int healthPort;
    public final Duration shutdownGrace;

    private PipelineConfig(Builder b) {
        this.dbUrl = b.dbUrl;
        this.dbUser = b.dbUser;
        this.dbPassword = b.dbPassword;
        this.dbPoolSize = b.dbPoolSize;
        this.sourceZoneForNaiveTimestamps = b.sourceZone;
        this.pipelineName = b.pipelineName;
        this.batchSize = b.batchSize;
        this.esHost = b.esHost;
        this.esPort = b.esPort;
        this.esScheme = b.esScheme;
        this.esUsername = b.esUsername;
        this.esPassword = b.esPassword;
        this.esApiKey = b.esApiKey;
        this.indexSubBatchSize = b.indexSubBatchSize;
        this.indexMaxBytesPerRequest = b.indexMaxBytesPerRequest;
        this.bulkMaxAttempts = b.bulkMaxAttempts;
        this.bulkBaseDelayMs = b.bulkBaseDelayMs;
        this.bulkMaxDelayMs = b.bulkMaxDelayMs;
        this.window = b.window;
        this.errorRateSpikeMultiplier = b.errorRateSpikeMultiplier;
        this.minEventsForRateRule = b.minEventsForRateRule;
        this.failedLoginThreshold = b.failedLoginThreshold;
        this.emaAlpha = b.emaAlpha;
        this.redisHost = b.redisHost;
        this.redisPort = b.redisPort;
        this.redisPassword = b.redisPassword;
        this.redisKeyPrefix = b.redisKeyPrefix;
        this.runInterval = b.runInterval;
        this.healthPort = b.healthPort;
        this.shutdownGrace = b.shutdownGrace;
        validate();
    }

    private void validate() {
        require(dbUrl != null && !dbUrl.isBlank(), "DB_URL is required");
        require(pipelineName != null && !pipelineName.isBlank(), "PIPELINE_NAME is required");
        require(batchSize > 0, "BATCH_SIZE must be > 0");
        require(indexSubBatchSize > 0, "INDEX_SUB_BATCH_SIZE must be > 0");
        require(indexMaxBytesPerRequest > 0, "INDEX_MAX_BYTES must be > 0");
        require(bulkMaxAttempts >= 1, "BULK_MAX_ATTEMPTS must be >= 1");
        require(esHost != null && !esHost.isBlank(), "ES_HOST is required");
        require(esPort > 0, "ES_PORT must be > 0");
        require(!window.isZero() && !window.isNegative(), "WINDOW must be a positive duration");
        require(emaAlpha > 0 && emaAlpha <= 1, "EMA_ALPHA must be in (0,1]");
        require(redisHost != null && !redisHost.isBlank(), "REDIS_HOST is required");
        require(!runInterval.isNegative(), "RUN_INTERVAL must be >= 0");
    }

    private static void require(boolean cond, String message) {
        if (!cond) {
            throw new IllegalStateException("Invalid configuration: " + message);
        }
    }

    /** Loads config from environment variables overlaid on the given properties (may be empty). */
    public static PipelineConfig load(Properties props) {
        Function<String, String> get = key -> resolve(key, props);
        Builder b = new Builder();
        b.dbUrl = orDefault(get, "db.url", "jdbc:mysql://localhost:3306/logs"
                + "?serverTimezone=UTC&connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true");
        b.dbUser = orDefault(get, "db.user", "root");
        b.dbPassword = orDefault(get, "db.password", "");
        b.dbPoolSize = intOr(get, "db.pool.size", 8);
        b.sourceZone = ZoneId.of(orDefault(get, "source.zone", "UTC"));
        b.pipelineName = orDefault(get, "pipeline.name", "app-logs");
        b.batchSize = intOr(get, "batch.size", 5000);
        b.esHost = orDefault(get, "es.host", "localhost");
        b.esPort = intOr(get, "es.port", 9200);
        b.esScheme = orDefault(get, "es.scheme", "http");
        b.esUsername = orDefault(get, "es.username", null);
        b.esPassword = orDefault(get, "es.password", null);
        b.esApiKey = orDefault(get, "es.api.key", null);
        b.indexSubBatchSize = intOr(get, "index.sub.batch.size", 500);
        b.indexMaxBytesPerRequest = longOr(get, "index.max.bytes", 5L * 1024 * 1024);
        b.bulkMaxAttempts = intOr(get, "bulk.max.attempts", 4);
        b.bulkBaseDelayMs = longOr(get, "bulk.base.delay.ms", 1_000L);
        b.bulkMaxDelayMs = longOr(get, "bulk.max.delay.ms", 30_000L);
        b.window = Duration.ofSeconds(longOr(get, "window.seconds", 300));
        b.errorRateSpikeMultiplier = doubleOr(get, "anomaly.spike.multiplier", 3.0);
        b.minEventsForRateRule = longOr(get, "anomaly.min.events", 50);
        b.failedLoginThreshold = longOr(get, "anomaly.failed.login.threshold", 20);
        b.emaAlpha = doubleOr(get, "anomaly.ema.alpha", 0.3);
        b.redisHost = orDefault(get, "redis.host", "localhost");
        b.redisPort = intOr(get, "redis.port", 6379);
        b.redisPassword = orDefault(get, "redis.password", null);
        b.redisKeyPrefix = orDefault(get, "redis.key.prefix", "pipeline:anomaly:");
        b.runInterval = Duration.ofSeconds(longOr(get, "run.interval.seconds", 10));
        b.healthPort = intOr(get, "health.port", 8080);
        b.shutdownGrace = Duration.ofSeconds(longOr(get, "shutdown.grace.seconds", 30));
        return new PipelineConfig(b);
    }

    private static String resolve(String dottedKey, Properties props) {
        String env = System.getenv(normalize(dottedKey));
        if (env != null && !env.isBlank()) {
            return env;
        }
        return props == null ? null : props.getProperty(dottedKey);
    }

    /** db.url -> DB_URL */
    static String normalize(String dottedKey) {
        return dottedKey.toUpperCase().replace('.', '_');
    }

    private static String orDefault(Function<String, String> get, String key, String def) {
        String v = get.apply(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    private static int intOr(Function<String, String> get, String key, int def) {
        String v = get.apply(key);
        return (v == null || v.isBlank()) ? def : Integer.parseInt(v.strip());
    }

    private static long longOr(Function<String, String> get, String key, long def) {
        String v = get.apply(key);
        return (v == null || v.isBlank()) ? def : Long.parseLong(v.strip());
    }

    private static double doubleOr(Function<String, String> get, String key, double def) {
        String v = get.apply(key);
        return (v == null || v.isBlank()) ? def : Double.parseDouble(v.strip());
    }

    public boolean redisAuthEnabled() {
        return redisPassword != null && !redisPassword.isBlank();
    }

    private static final class Builder {
        String dbUrl, dbUser, dbPassword, pipelineName;
        int dbPoolSize, batchSize, esPort, indexSubBatchSize, bulkMaxAttempts, redisPort, healthPort;
        long indexMaxBytesPerRequest, bulkBaseDelayMs, bulkMaxDelayMs,
                minEventsForRateRule, failedLoginThreshold;
        String esHost, esScheme, esUsername, esPassword, esApiKey, redisHost, redisPassword, redisKeyPrefix;
        double errorRateSpikeMultiplier, emaAlpha;
        ZoneId sourceZone;
        Duration window, runInterval, shutdownGrace;
    }
}
