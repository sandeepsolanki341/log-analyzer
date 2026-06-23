package com.sandeep.pipeline.runner;

import com.sandeep.pipeline.analyze.AnomalyDetector;
import com.sandeep.pipeline.analyze.Analyzer;
import com.sandeep.pipeline.analyze.BaselineStore;
import com.sandeep.pipeline.analyze.Classifier;
import com.sandeep.pipeline.analyze.Correlator;
import com.sandeep.pipeline.analyze.Enricher;
import com.sandeep.pipeline.analyze.Fingerprinter;
import com.sandeep.pipeline.analyze.InMemoryBaselineStore;
import com.sandeep.pipeline.analyze.RedisBaselineStore;
import com.sandeep.pipeline.analyze.SlidingWindowAggregator;
import com.sandeep.pipeline.config.PipelineConfig;
import com.sandeep.pipeline.extract.CheckpointStore;
import com.sandeep.pipeline.extract.LogExtractor;
import com.sandeep.pipeline.index.BackoffPolicy;
import com.sandeep.pipeline.index.BulkIndexer;
import com.sandeep.pipeline.index.ElasticsearchTransport;
import com.sandeep.pipeline.index.IndexDeadLetterStore;
import com.sandeep.pipeline.index.RealElasticsearchTransport;
import com.sandeep.pipeline.parse.DeadLetterStore;
import com.sandeep.pipeline.parse.LogParser;
import com.sandeep.pipeline.util.Metrics;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.sql.DataSource;

/**
 * Composition root. Builds the entire object graph from a validated {@link PipelineConfig} and owns
 * the closeable resources (datasource, Redis pool, ES transport). No framework / DI container — the
 * graph is small and explicit construction keeps startup obvious and debuggable.
 */
public final class PipelineWiring implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PipelineWiring.class);

    private final HikariDataSource dataSource;
    private final JedisPool jedisPool;          // nullable (in-memory fallback)
    private final ElasticsearchTransport transport;
    private final LogExtractor extractor;
    private final PipelineChain chain;
    private final Metrics metrics;

    private PipelineWiring(HikariDataSource ds, JedisPool jedisPool, ElasticsearchTransport transport,
                           LogExtractor extractor, PipelineChain chain, Metrics metrics) {
        this.dataSource = ds;
        this.jedisPool = jedisPool;
        this.transport = transport;
        this.extractor = extractor;
        this.chain = chain;
        this.metrics = metrics;
    }

    public static PipelineWiring build(PipelineConfig cfg, Metrics metrics) {
        // --- DataSource (HikariCP) ---
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(cfg.dbUrl);
        hc.setUsername(cfg.dbUser);
        hc.setPassword(cfg.dbPassword);
        hc.setMaximumPoolSize(cfg.dbPoolSize);
        hc.setPoolName("pipeline-pool");
        hc.setAutoCommit(true);
        HikariDataSource ds = new HikariDataSource(hc);
        log.info("HikariCP pool initialized (size={})", cfg.dbPoolSize);

        // --- Extraction ---
        CheckpointStore checkpoint = new CheckpointStore(ds, cfg.pipelineName);
        LogExtractor extractor = new LogExtractor(ds, checkpoint, cfg.batchSize);

        // --- Parse ---
        LogParser parser = new LogParser(cfg.sourceZoneForNaiveTimestamps);
        DeadLetterStore parseDeadLetters = new DeadLetterStore(ds);

        // --- Analyze ---
        Classifier classifier = new Classifier();
        Fingerprinter fingerprinter = new Fingerprinter();
        SlidingWindowAggregator aggregator = new SlidingWindowAggregator(cfg.window);
        Enricher enricher = new Enricher(null, null, System.getenv("DEPLOYMENT_VERSION"), 10_000);
        Correlator correlator = new Correlator();

        BaselineStoreHolder baseline = buildBaselineStore(cfg);
        AnomalyDetector detector = new AnomalyDetector(
                cfg.errorRateSpikeMultiplier, cfg.minEventsForRateRule,
                cfg.failedLoginThreshold, cfg.emaAlpha, baseline.store);
        Analyzer analyzer = new Analyzer(classifier, fingerprinter, aggregator,
                detector, enricher, correlator);

        // --- Index ---
        ElasticsearchTransport transport = new RealElasticsearchTransport(
                cfg.esHost, cfg.esPort, cfg.esScheme, cfg.esUsername, cfg.esPassword, cfg.esApiKey);
        IndexDeadLetterStore indexDeadLetters = new IndexDeadLetterStore(ds);
        BackoffPolicy backoff = new BackoffPolicy(cfg.bulkMaxAttempts, cfg.bulkBaseDelayMs, cfg.bulkMaxDelayMs);
        BulkIndexer indexer = new BulkIndexer(transport, indexDeadLetters, backoff,
                cfg.indexSubBatchSize, cfg.indexMaxBytesPerRequest, metrics);

        PipelineChain chain = new PipelineChain(parser, parseDeadLetters, analyzer, indexer, metrics);

        return new PipelineWiring(ds, baseline.pool, transport, extractor, chain, metrics);
    }

    private record BaselineStoreHolder(BaselineStore store, JedisPool pool) {
    }

    private static BaselineStoreHolder buildBaselineStore(PipelineConfig cfg) {
        try {
            JedisPoolConfig pc = new JedisPoolConfig();
            pc.setMaxTotal(8);
            pc.setMaxIdle(4);
            JedisPool pool = cfg.redisAuthEnabled()
                    ? new JedisPool(pc, cfg.redisHost, cfg.redisPort, 2000, cfg.redisPassword)
                    : new JedisPool(pc, cfg.redisHost, cfg.redisPort, 2000);
            // Probe once so a misconfigured Redis fails fast at startup rather than silently degrading.
            try (var j = pool.getResource()) {
                j.ping();
            }
            log.info("Redis baseline store connected at {}:{}", cfg.redisHost, cfg.redisPort);
            return new BaselineStoreHolder(new RedisBaselineStore(pool, cfg.redisKeyPrefix), pool);
        } catch (Exception e) {
            log.warn("Redis unavailable ({}); falling back to in-memory baseline (resets on restart). "
                    + "Anomaly detection will warm up after each restart.", e.getMessage());
            return new BaselineStoreHolder(new InMemoryBaselineStore(), null);
        }
    }

    public LogExtractor extractor() {
        return extractor;
    }

    public PipelineChain chain() {
        return chain;
    }

    /** Lightweight DB connectivity probe for readiness. */
    public boolean dbReachable() {
        try (var c = dataSource.getConnection()) {
            return c.isValid(2);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() {
        closeQuietly("es-transport", transport);
        if (jedisPool != null) {
            closeQuietly("redis-pool", jedisPool);
        }
        closeQuietly("datasource", dataSource);
    }

    private void closeQuietly(String name, AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception e) {
            log.warn("Error closing {}: {}", name, e.getMessage());
        }
    }
}
