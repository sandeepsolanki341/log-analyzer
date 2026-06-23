package com.sandeep.pipeline.extract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

/**
 * Reads new rows from {@code app_logs} in bounded batches using strict keyset pagination and hands
 * each batch to a downstream {@link BatchConsumer}.
 *
 * <h2>Why keyset pagination (and never OFFSET)</h2>
 * Each query is {@code SELECT ... WHERE id > ? ORDER BY id LIMIT ?}. Because {@code id} is the
 * indexed PK, the DB seeks directly to the first row past the cursor and reads forward {@code LIMIT}
 * rows — constant cost regardless of table depth. {@code OFFSET n} would force the DB to generate and
 * discard {@code n} rows per batch, getting linearly slower as the table grows.
 *
 * <h2>The single-run catch-up loop</h2>
 * One {@link #runOnce} loops internally: read a batch, hand it downstream, advance the durable
 * checkpoint to the batch's true max id, read again. Stops when a batch returns fewer rows than
 * {@code batchSize} (drained to real-time). Per-batch checkpointing means a mid-run crash replays
 * only the unconfirmed batch.
 *
 * <h2>Interruptibility</h2>
 * The loop checks the interrupt flag between batches so a graceful shutdown stops promptly without
 * advancing past an unprocessed batch.
 *
 * <h2>Note on fetch size</h2>
 * Each batch is already bounded by {@code LIMIT batchSize}, so the result set is a small bounded
 * page — we are NOT row-streaming (true MySQL streaming requires {@code fetchSize == Integer.MIN_VALUE},
 * which is the wrong tool here). A bounded page is exactly what keyset pagination wants.
 */
public class LogExtractor {

    private static final Logger log = LoggerFactory.getLogger(LogExtractor.class);

    private static final String SELECT_BATCH_SQL =
            "SELECT id, ts, level, service, message, user_id, ip, status_code, latency_ms, stack_trace "
                    + "FROM app_logs WHERE id > ? ORDER BY id ASC LIMIT ?";

    private static final String MAX_ID_SQL = "SELECT MAX(id) FROM app_logs";

    private final DataSource dataSource;
    private final CheckpointStore checkpointStore;
    private final int batchSize;

    public LogExtractor(DataSource dataSource, CheckpointStore checkpointStore, int batchSize) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        if (checkpointStore == null) {
            throw new IllegalArgumentException("checkpointStore must not be null");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
        }
        this.dataSource = dataSource;
        this.checkpointStore = checkpointStore;
        this.batchSize = batchSize;
    }

    /** Executes one full catch-up pass. */
    public long runOnce(BatchConsumer consumer) {
        return runOnce(consumer, null);
    }

    /**
     * Executes one full catch-up pass, optionally reporting the remaining backlog after the first
     * batch via {@code backlogReporter} (for metrics).
     *
     * @param consumer        downstream handler; must throw on failure.
     * @param backlogReporter optional sink for an estimated backlog (rows still behind), may be null.
     * @return total records extracted and confirmed across all batches this run.
     */
    public long runOnce(BatchConsumer consumer, LongConsumer backlogReporter) {
        if (consumer == null) {
            throw new IllegalArgumentException("consumer must not be null");
        }

        long cursor = checkpointStore.read();
        long totalProcessed = 0;
        int batchNumber = 0;

        if (backlogReporter != null) {
            try {
                long maxId = currentMaxId();
                backlogReporter.accept(Math.max(0, maxId - cursor));
            } catch (Exception e) {
                log.debug("Backlog estimate failed (non-fatal): {}", e.getMessage());
            }
        }

        log.info("Extraction run starting from checkpoint id={} (batchSize={})", cursor, batchSize);

        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                log.info("Extraction interrupted; stopping after {} batch(es), checkpoint at {}",
                        batchNumber, cursor);
                break;
            }

            List<RawLogRecord> batch = fetchBatch(cursor);
            if (batch.isEmpty()) {
                log.debug("No rows beyond id={}; nothing to process.", cursor);
                break;
            }

            long maxIdInBatch = batch.get(batch.size() - 1).id();
            batchNumber++;
            log.debug("Fetched batch #{}: {} rows, id range ({} .. {}]",
                    batchNumber, batch.size(), cursor, maxIdInBatch);

            try {
                consumer.accept(batch);
            } catch (Exception e) {
                log.error("Downstream consumer failed on batch #{} (id range ({} .. {}]); "
                        + "checkpoint NOT advanced.", batchNumber, cursor, maxIdInBatch, e);
                throw new PipelineExtractionException(
                        "Downstream processing failed for batch ending at id " + maxIdInBatch, e);
            }

            checkpointStore.advanceTo(maxIdInBatch);
            cursor = maxIdInBatch;
            totalProcessed += batch.size();

            if (batch.size() < batchSize) {
                log.debug("Batch #{} returned {} (< limit {}); caught up to real-time.",
                        batchNumber, batch.size(), batchSize);
                break;
            }
        }

        log.info("Extraction run complete: {} record(s) across {} batch(es); checkpoint id={}",
                totalProcessed, batchNumber, cursor);
        return totalProcessed;
    }

    private long currentMaxId() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MAX_ID_SQL);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new PipelineExtractionException("Failed to read MAX(id)", e);
        }
    }

    private List<RawLogRecord> fetchBatch(long afterId) {
        List<RawLogRecord> rows = new ArrayList<>(Math.min(batchSize, 1024));
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BATCH_SQL)) {

            ps.setLong(1, afterId);
            ps.setInt(2, batchSize);
            log.debug("Executing keyset query: id > {} LIMIT {}", afterId, batchSize);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(mapRow(rs));
                }
            }
            return rows;
        } catch (SQLException e) {
            log.error("SQL error fetching batch after id={}", afterId, e);
            throw new PipelineExtractionException("Failed to fetch batch after id " + afterId, e);
        }
    }

    private RawLogRecord mapRow(ResultSet rs) throws SQLException {
        long id = rs.getLong("id");

        long userIdRaw = rs.getLong("user_id");
        Long userId = rs.wasNull() ? null : userIdRaw;

        int statusRaw = rs.getInt("status_code");
        Integer statusCode = rs.wasNull() ? null : statusRaw;

        long latencyRaw = rs.getLong("latency_ms");
        Long latencyMs = rs.wasNull() ? null : latencyRaw;

        return new RawLogRecord(
                id,
                rs.getTimestamp("ts"),
                rs.getString("level"),
                rs.getString("service"),
                rs.getString("message"),
                userId,
                rs.getString("ip"),
                statusCode,
                latencyMs,
                rs.getString("stack_trace"));
    }
}
