# Architecture notes

See the original design narrative for the layer-by-layer rationale. This file records the
production hardening applied on top of the reference implementation.

## Correctness fixes
1. **Event-time windowing** (`SlidingWindowAggregator`). Entries are stamped/evicted by event time
   with a monotonic watermark, not wall-clock. Fixes backlog/replay distortion of error rates and
   EMA baselines. Recording is idempotent per `sourceId` so replays don't double-count.
2. **Durable anomaly baseline** (`AnomalyDetector` + `RedisBaselineStore`). The trailing EMA is
   persisted, so anomaly detection survives restarts instead of cold-starting.
3. **Deterministic alert ids** with a dedicated `app-alerts-*` index — replays dedupe.
4. **Monotonic checkpoint** via SQL `GREATEST(...)` in the UPSERT — no read-modify-write TOCTOU.

## Robustness fixes
5. **Bulk sub-batching** by document count and byte size in `BulkIndexer` — avoids
   `http.max_content_length` overruns and heap spikes independent of fetch size.
6. **Interrupt-aware backoff** — `BackoffPolicy.sleep` throws `InterruptedException`; the indexer
   aborts to `BulkIndexException` so shutdown is prompt and the checkpoint doesn't advance.
7. **Non-overlapping scheduler** (`scheduleWithFixedDelay`, single thread) protects the
   single-writer aggregator/checkpoint even when a run blocks on ES backoff.
8. **Robust parsing** — Jackson replaces the hand-rolled JSON reader; nested objects flatten to
   dotted keys instead of dead-lettering.

## Operability
9. Liveness/readiness/metrics HTTP server; Prometheus meters; graceful SIGTERM shutdown.
10. Env/properties config with validation; HikariCP pool; fail-fast Redis probe with safe fallback.

## Known tradeoffs
- **Single-threaded throughput.** Synchronous handoff gives free backpressure but caps throughput at
  serial `fetch + parse + analyze + index`. Horizontal scaling requires partitioning the source by
  id range with separate `pipeline_name`s (the checkpoint is per-pipeline).
- **Fingerprint digit normalization** groups e.g. HTTP 500 and 503 together; category/severity
  fields remain available to distinguish them.
