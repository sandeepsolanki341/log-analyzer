# Sandeep Log Analysis Pipeline

A production-grade Java ETL service that streams application logs from **MySQL** into
**Elasticsearch** (visualized in **Kibana**), enriching and analyzing them in flight. It is built
around four reliability guarantees and event-time anomaly detection.

```
MySQL (raw) → Extractor → Parser → Analyzer → Bulk Indexer → Elasticsearch → Kibana
                 │            │          │            │
            checkpoint   dead-letter  event-time   dead-letter
            (keyset)     (parse)      window +     (index) +
                                      Redis EMA    sub-batching
```

## Reliability model

| Guarantee | How |
|---|---|
| **At-least-once delivery** | Durable `CheckpointStore` high-water mark advanced only after a batch is confirmed downstream. |
| **Effectively exactly-once storage** | Deterministic Elasticsearch `_id` (`applog-<rowId>`) ⇒ replays overwrite, never duplicate. |
| **Poison-pill immunity** | Per-row parse dead-letter + per-document index dead-letter. One bad row/doc never halts the pipeline. |
| **Natural backpressure** | Synchronous batch handoff; when Elasticsearch slows, the indexer blocks, which pauses extraction instead of growing memory. |

## What makes this more than a reference implementation

- **Event-time sliding window.** Aggregation is keyed on each event's own timestamp with a
  watermark, so error rates stay correct during backlog catch-up and replay (processing-time windows
  silently lie exactly when an incident is unfolding). Recording is idempotent per `sourceId`, so a
  replayed batch never double-counts.
- **Durable anomaly baseline.** The trailing EMA used for spike detection is persisted in **Redis**,
  so a restart resumes from the learned baseline instead of cold-starting and missing (or inventing)
  a spike during warm-up. Falls back to in-memory if Redis is unavailable.
- **Bulk sub-batching by count *and* bytes.** A large extractor batch is split into ES bulk requests
  bounded by both document count and byte size, preventing `http.max_content_length` overruns and
  heap spikes independent of the fetch size.
- **Interrupt-aware indexing & non-overlapping scheduling.** Backoff propagates interrupts so
  shutdown is prompt; a single-thread `scheduleWithFixedDelay` guarantees runs never overlap
  (protecting the single-writer aggregator/checkpoint).
- **Deterministic, idempotent alerts.** Synthetic alert documents get a stable id
  (`alert-<rule>-<subject>-<epochSecond>`) and route to a separate `app-alerts-*` index.
- **Operational surface.** Liveness/readiness endpoints and Prometheus metrics over a tiny built-in
  HTTP server; graceful SIGTERM shutdown.

## Quick start (full local stack)

```bash
docker compose up --build
```

This brings up MySQL (with schema + sample rows), Elasticsearch, Kibana, Redis, applies the ES index
template, and starts the pipeline. Then:

- Kibana: http://localhost:5601 (create a data view for `app-logs-*` and `app-alerts-*`)
- Pipeline health: http://localhost:8080/health/ready
- Metrics: http://localhost:8080/metrics

Insert more logs into `app_logs` and watch them appear in Kibana within a few seconds.

## Build & test

```bash
mvn clean test        # unit tests
mvn verify            # + Testcontainers integration test (needs Docker)
mvn package           # runnable fat jar at target/log-analysis-pipeline.jar
```

Run the jar directly (configure via env vars — see below):

```bash
DB_URL='jdbc:mysql://localhost:3306/logs?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true' \
DB_USER=root DB_PASSWORD=secret \
ES_HOST=localhost ES_PORT=9200 \
REDIS_HOST=localhost \
java -jar target/log-analysis-pipeline.jar
```

## Configuration

Every key has a default (see `src/main/resources/application.properties`) and is overridable by the
matching environment variable (`db.url` → `DB_URL`). Secrets should come from the environment / a
secrets manager, not the properties file.

| Env var | Default | Notes |
|---|---|---|
| `DB_URL` | `jdbc:mysql://localhost:3306/logs?...` | Pin the session to UTC (see Timestamps). |
| `DB_USER` / `DB_PASSWORD` | `root` / _(empty)_ | |
| `DB_POOL_SIZE` | `8` | HikariCP max pool size. |
| `SOURCE_ZONE` | `UTC` | Zone for zoneless `DATETIME` timestamps. |
| `PIPELINE_NAME` | `app-logs` | Checkpoint row key. |
| `BATCH_SIZE` | `5000` | Rows per extraction batch (keyset `LIMIT`). |
| `ES_HOST` / `ES_PORT` / `ES_SCHEME` | `localhost` / `9200` / `http` | |
| `ES_USERNAME` / `ES_PASSWORD` / `ES_API_KEY` | _(none)_ | API key takes precedence over basic auth. |
| `INDEX_SUB_BATCH_SIZE` | `500` | Max docs per ES bulk request. |
| `INDEX_MAX_BYTES` | `5242880` | Max bytes per ES bulk request. |
| `BULK_MAX_ATTEMPTS` | `4` | Total attempts incl. first. |
| `BULK_BASE_DELAY_MS` / `BULK_MAX_DELAY_MS` | `1000` / `30000` | Exponential backoff with full jitter. |
| `WINDOW_SECONDS` | `300` | Event-time analysis window width. |
| `ANOMALY_SPIKE_MULTIPLIER` | `3.0` | Fire when rate > multiplier × trailing EMA. |
| `ANOMALY_MIN_EVENTS` | `50` | Min window volume before the rate rule trusts itself. |
| `ANOMALY_FAILED_LOGIN_THRESHOLD` | `20` | Failed logins per IP that trip a security alert. |
| `ANOMALY_EMA_ALPHA` | `0.3` | Baseline smoothing factor (0,1]. |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` | `localhost` / `6379` / _(none)_ | Baseline store. |
| `RUN_INTERVAL_SECONDS` | `10` | Delay between (non-overlapping) runs. |
| `HEALTH_PORT` | `8080` | Liveness/readiness/metrics. |
| `SHUTDOWN_GRACE_SECONDS` | `30` | Graceful shutdown budget. |

### Timestamps (read this)

`app_logs.ts` is a zoneless `DATETIME`, interpreted in `SOURCE_ZONE` (default UTC). If your services
write a MySQL `TIMESTAMP` column instead, MySQL stores it as UTC and the JDBC driver converts to the
session zone — so pin the session to UTC in the JDBC URL
(`connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true`) and keep `SOURCE_ZONE=UTC`.
Mismatching these shifts every event by the zone offset.

## Data model evolution

`RawLogRecord` (untrusted DB row) → `LogEvent` (clean, UTC `Instant`, canonical `LogLevel`,
extracted fields) → `AnalyzedEvent` (+ classification, fingerprint, enrichment, trace id,
deterministic `esId`, time-based `targetIndex`).

## Schema & index template

- MySQL: `deploy/sql/schema.sql` (source table, checkpoint, two dead-letter tables).
- Elasticsearch: `deploy/elasticsearch/index-template.json` (apply with `apply-template.sh` before
  first ingest so `@timestamp` is a `date` and key fields are `keyword`).

## Operations

- **Readiness** returns 200 only after at least one successful run and a live DB connection.
- **Dead-letter monitoring**: alert on growth of `parse_dead_letter` (usually a service changed log
  format) and `index_dead_letter` (mapping conflicts). Both are idempotent and safe to reprocess.
- **Metrics** (Prometheus): rows extracted/parsed, events analyzed, alerts raised, docs
  indexed/dead-lettered, bulk retries, index/run latency, last-run timestamp, backlog estimate.

## Project layout

```
src/main/java/com/sandeep/pipeline/
  extract/   keyset extractor, checkpoint, raw record
  parse/     parser (Jackson, nested-JSON flattening), dead-letter, log event/level
  analyze/   classifier, fingerprinter, enricher, correlator,
             event-time window, anomaly detector, Redis/in-memory baseline
  index/     bulk indexer (sub-batching, backoff, triage), ES transport, dead-letter, JSON writer
  config/    env/properties-driven config with validation
  runner/    chain (BatchConsumer), wiring (composition root), scheduler, Main
  health/    liveness/readiness/metrics HTTP server
  util/      shared Jackson mapper, Micrometer metrics
deploy/      sql schema + seed, ES index template + apply script
```

## License

MIT — see `LICENSE`. Architecture hardening notes: `docs/ARCHITECTURE.md`.
