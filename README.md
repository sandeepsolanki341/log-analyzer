# Real-Time Multi-Source Log Analytics Platform

A production-grade Java ETL platform that ingests application logs from **four heterogeneous sources** — **MySQL**, **MongoDB**, **Redis Streams**, and **Kafka** — into **Elasticsearch** (visualized in **Kibana**), with in-flight enrichment, event-time anomaly detection, and a built-in operational surface (health, readiness, Prometheus metrics).

Built in **plain Java 21** — no framework runtime. Dependencies are wired by hand in an explicit composition root, which keeps startup fast, the dependency graph readable, and the failure modes obvious.

Built during an internship at **Jio Platforms Limited** (Jun–Jul 2026).

## Architecture

The platform uses a **hub-and-spoke architecture** with **Kafka as the durable ingestion backbone**. Each source has a dedicated connector that publishes normalized log events to Kafka, which then feeds a horizontally scalable consumer group running a three-stage pipeline: **parse → analyze → bulk index**.

```
┌─────────────┐
│   MySQL     │──► MySQL Connector (keyset pagination)
└─────────────┘                                          ┌──────────┐
┌─────────────┐                                          │          │    ┌────────────────┐
│  MongoDB    │──► MongoDB Connector (ObjectId cursors) ─►│  Kafka   │───►│ Consumer Group  │
└─────────────┘                                          │ (hub)    │    │                │
┌─────────────┐                                          │          │    │  Parse          │
│Redis Streams│──► Redis Connector (stream entry IDs)  ─►│          │    │  ↓              │
└─────────────┘                                          │          │    │  Analyze        │
┌─────────────┐                                          │          │    │  ↓              │
│External Kafka│──► Kafka Connector (consumer offsets) ─►│          │    │  Bulk Index     │
└─────────────┘                                          └──────────┘    └───────┬────────┘
                                                                                │
                                                              ┌─────────────────┼──────────────┐
                                                              │                 │              │
                                                              ▼                 ▼              ▼
                                                        Elasticsearch       Kibana       Health / Metrics
                                                        (app-logs-*)      (visualize)    HTTP endpoints
                                                        (app-alerts-*)                   (Prometheus)
```

## Reliability Model

| Guarantee | How |
|---|---|
| **At-least-once delivery** | Each connector maintains a durable checkpoint (MySQL row IDs, MongoDB ObjectId cursors, Redis stream entry IDs, Kafka consumer offsets). Checkpoints advance only after downstream acknowledgment. |
| **Effectively exactly-once storage** | Deterministic Elasticsearch `_id` per document (`applog-<source>-<sourceId>`). Crash replays overwrite, never duplicate. |
| **Poison-pill immunity** | Per-record parse dead-letter + per-document index dead-letter. One bad record never halts any pipeline stage. |
| **Backpressure** | Kafka consumer pause/resume: when Elasticsearch or downstream processing slows, consumers pause partition fetching instead of growing memory. Jittered-backoff bulk retries on transient ES failures. |
| **Dead-letter isolation** | Poison messages that exhaust retries are routed to dead-letter topics/tables for later inspection without blocking the main pipeline. |

## Key Design Decisions

- **Hub-and-spoke over point-to-point.** Rather than wiring each source directly to Elasticsearch, every connector publishes to a single Kafka topic. This decouples ingestion rate from indexing rate, lets the consumer group scale independently of the sources, and allows adding a new source without touching downstream processing.

- **Unified connector pattern.** All four connectors share the same interface: fetch a batch → normalize to a common log event schema → publish to Kafka → commit checkpoint. Source-specific logic (keyset pagination for MySQL, ObjectId cursors for MongoDB, `XREAD` with entry IDs for Redis Streams, standard consumer offsets for Kafka) is encapsulated per connector.

- **Event-time sliding window anomaly detection.** Aggregation is keyed on each event's own timestamp with a watermark, so error rates stay correct during backlog catch-up and replay. Processing-time windows silently lie exactly when an incident is unfolding. Recording is idempotent per `sourceId`.

- **Durable anomaly baseline.** The trailing EMA used for spike detection is persisted in **Redis**, so a restart resumes from the learned baseline instead of cold-starting and missing (or inventing) a spike during warm-up. Falls back to in-memory if Redis is unavailable.

- **Bulk sub-batching by count *and* bytes.** A large consumer batch is split into ES bulk requests bounded by both document count and byte size, preventing `http.max_content_length` overruns and heap spikes.

- **Deterministic, idempotent alerts.** Synthetic alert documents get a stable id (`alert-<rule>-<subject>-<epochSecond>`) and route to a separate `app-alerts-*` index.

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Runtime | Plain Java, manual DI (no framework) |
| Sources | MySQL, MongoDB, Redis Streams, Apache Kafka |
| Message Broker | Apache Kafka (ingestion hub) |
| Search & Analytics | Elasticsearch, Kibana |
| Anomaly Baseline | Redis (EMA persistence) |
| Ops Surface | Built-in HTTP server (health, readiness, metrics) |
| Containerization | Docker, Docker Compose |
| CI/CD | GitHub Actions |
| Testing | JUnit, Testcontainers |
| Monitoring | Prometheus metrics, liveness/readiness endpoints |

## Quick Start (Full Local Stack)

```bash
docker compose up --build
```

This brings up MySQL, MongoDB, Redis, Kafka (with Zookeeper), Elasticsearch, Kibana, and the pipeline service. Then:

- **Kibana:** [http://localhost:5601](http://localhost:5601) — create data views for `app-logs-*` and `app-alerts-*`
- **Pipeline health:** [http://localhost:8080/health/ready](http://localhost:8080/health/ready)
- **Metrics:** [http://localhost:8080/metrics](http://localhost:8080/metrics)

## Build & Test

```bash
mvn clean test        # unit tests
mvn verify            # + Testcontainers integration tests (needs Docker)
mvn package           # runnable fat jar at target/log-analysis-pipeline.jar
```

Run the jar directly (configure via env vars):

```bash
KAFKA_BOOTSTRAP=localhost:9092 \
DB_URL='jdbc:mysql://localhost:3306/logs?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true' \
DB_USER=root DB_PASSWORD=secret \
MONGO_URI='mongodb://localhost:27017/logs' \
REDIS_HOST=localhost \
ES_HOST=localhost ES_PORT=9200 \
java -jar target/log-analysis-pipeline.jar
```

## Configuration

Every key has a default (see `src/main/resources/application.properties`) and is overridable by the matching environment variable. Secrets should come from the environment or a secrets manager.

### Core

| Env var | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `KAFKA_CONSUMER_GROUP` | `log-analytics` | Consumer group for the processing pipeline |
| `KAFKA_INGEST_TOPIC` | `raw-logs` | Topic all connectors publish to |
| `DB_URL` | `jdbc:mysql://localhost:3306/logs?...` | MySQL source; pin session to UTC |
| `DB_USER` / `DB_PASSWORD` | `root` / *(empty)* | |
| `MONGO_URI` | `mongodb://localhost:27017/logs` | MongoDB source connection |
| `MONGO_COLLECTION` | `app_logs` | Collection to tail |
| `REDIS_STREAM_KEY` | `app:logs` | Redis Stream key to consume |
| `ES_HOST` / `ES_PORT` / `ES_SCHEME` | `localhost` / `9200` / `http` | |

### Reliability & Tuning

| Env var | Default | Notes |
|---|---|---|
| `BATCH_SIZE` | `5000` | Records per connector fetch cycle |
| `INDEX_SUB_BATCH_SIZE` | `500` | Max docs per ES bulk request |
| `INDEX_MAX_BYTES` | `5242880` | Max bytes per ES bulk request |
| `BULK_MAX_ATTEMPTS` | `4` | Total attempts incl. first |
| `BULK_BASE_DELAY_MS` / `BULK_MAX_DELAY_MS` | `1000` / `30000` | Exponential backoff with full jitter |

### Anomaly Detection

| Env var | Default | Notes |
|---|---|---|
| `WINDOW_SECONDS` | `300` | Event-time sliding window width |
| `ANOMALY_SPIKE_MULTIPLIER` | `3.0` | Fire when rate > multiplier × trailing EMA |
| `ANOMALY_MIN_EVENTS` | `50` | Min window volume before the rule activates |
| `ANOMALY_EMA_ALPHA` | `0.3` | Baseline smoothing factor (0,1] |
| `ANOMALY_FAILED_LOGIN_THRESHOLD` | `20` | Failed logins per IP that trip a security alert |

## Data Model Evolution

```
RawLogRecord (untrusted source row/document)
  → LogEvent (clean, UTC Instant, canonical LogLevel, extracted fields)
    → AnalyzedEvent (+ classification, fingerprint, enrichment, trace id,
                      deterministic esId, time-based targetIndex)
```

## Operations

- **Readiness** returns 200 only after at least one successful run and live connections to all configured sources.
- **Dead-letter monitoring:** alert on growth of dead-letter topics/tables — parse dead-letters usually indicate a source changed its log format; index dead-letters indicate ES mapping conflicts.
- **Metrics (Prometheus):** records extracted per source, events parsed, events analyzed, alerts raised, docs indexed/dead-lettered, bulk retries, consumer lag, dispatch latency, last-run timestamp.

## Project Layout

```
src/main/java/com/sandeep/pipeline/
  connector/   source connectors (MySQL, MongoDB, Redis, Kafka) + unified interface
  extract/     keyset extractor, checkpoint store, raw record
  parse/       parser (Jackson, nested-JSON flattening), dead-letter, log event/level
  analyze/     classifier, fingerprinter, enricher, correlator,
               event-time window, anomaly detector, Redis/in-memory baseline
  index/       bulk indexer (sub-batching, backoff, triage), ES transport, dead-letter
  config/      env/properties-driven config with validation
  runner/      chain (BatchConsumer), wiring (composition root), scheduler, Main
  health/      liveness/readiness/metrics HTTP server (built-in, no framework)
  util/        shared Jackson mapper, Micrometer metrics
deploy/        docker-compose, sql schema + seed, ES index template, Kafka topic setup
docs/          ARCHITECTURE.md
```

## License

MIT — see `LICENSE`.
