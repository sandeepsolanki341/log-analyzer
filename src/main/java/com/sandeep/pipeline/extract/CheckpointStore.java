package com.sandeep.pipeline.extract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Durable bookmark for the extraction pipeline.
 *
 * <p>Persists a single high-water mark — the highest {@code app_logs.id} fully processed and
 * confirmed downstream — into the {@code pipeline_state} table. Only ever advanced <em>after</em> a
 * batch is confirmed downstream; combined with deterministic document ids, this yields at-least-once
 * delivery that behaves like exactly-once.
 *
 * <h2>Concurrency hardening</h2>
 * Monotonicity is enforced <em>in the database</em> via {@code GREATEST(...)} in the UPSERT, not by
 * a read-then-write in Java. This removes the prior TOCTOU window: even if two writers race (against
 * the documented single-writer assumption), the stored mark can never move backwards. A row lock is
 * still recommended for true multi-writer deployments, but the GREATEST guard makes the common case
 * safe by construction.
 *
 * <pre>{@code
 *   CREATE TABLE pipeline_state (
 *       pipeline_name     VARCHAR(128) NOT NULL,
 *       last_processed_id BIGINT       NOT NULL DEFAULT 0,
 *       updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
 *                                      ON UPDATE CURRENT_TIMESTAMP,
 *       PRIMARY KEY (pipeline_name)
 *   );
 * }</pre>
 */
public class CheckpointStore {

    private static final Logger log = LoggerFactory.getLogger(CheckpointStore.class);

    private static final long INITIAL_HIGH_WATER_MARK = 0L;

    private static final String SELECT_SQL =
            "SELECT last_processed_id FROM pipeline_state WHERE pipeline_name = ?";

    /**
     * Insert-or-update in a single atomic statement. {@code GREATEST} makes the advance monotonic at
     * the storage layer: a stale or out-of-order call can never rewind the mark.
     */
    private static final String UPSERT_SQL =
            "INSERT INTO pipeline_state (pipeline_name, last_processed_id) VALUES (?, ?) "
                    + "ON DUPLICATE KEY UPDATE last_processed_id = GREATEST(last_processed_id, VALUES(last_processed_id))";

    private final DataSource dataSource;
    private final String pipelineName;

    public CheckpointStore(DataSource dataSource, String pipelineName) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        if (pipelineName == null || pipelineName.isBlank()) {
            throw new IllegalArgumentException("pipelineName must not be null or blank");
        }
        this.dataSource = dataSource;
        this.pipelineName = pipelineName;
    }

    /**
     * Reads the current high-water mark.
     *
     * @return the last processed id, or {@code 0} if no checkpoint row exists yet.
     * @throws PipelineExtractionException if the read fails.
     */
    public long read() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {

            ps.setString(1, pipelineName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long mark = rs.getLong("last_processed_id");
                    log.debug("Read checkpoint for pipeline '{}': {}", pipelineName, mark);
                    return mark;
                }
                log.info("No checkpoint row for pipeline '{}'; starting from {}",
                        pipelineName, INITIAL_HIGH_WATER_MARK);
                return INITIAL_HIGH_WATER_MARK;
            }
        } catch (SQLException e) {
            log.error("Failed to read checkpoint for pipeline '{}'", pipelineName, e);
            throw new PipelineExtractionException(
                    "Could not read checkpoint for pipeline '" + pipelineName + "'", e);
        }
    }

    /**
     * Atomically advances the high-water mark to {@code newMark} (monotonic via {@code GREATEST}).
     *
     * @param newMark the highest id confirmed processed in the batch.
     * @throws PipelineExtractionException if the update fails.
     */
    public void advanceTo(long newMark) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
                ps.setString(1, pipelineName);
                ps.setLong(2, newMark);
                ps.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                safeRollback(conn);
                throw e;
            }
            log.debug("Advanced checkpoint for pipeline '{}' -> {} (monotonic)", pipelineName, newMark);
        } catch (SQLException e) {
            log.error("Failed to advance checkpoint for pipeline '{}' to {}", pipelineName, newMark, e);
            throw new PipelineExtractionException(
                    "Could not advance checkpoint for pipeline '" + pipelineName + "'", e);
        } finally {
            restoreAndClose(conn);
        }
    }

    private void safeRollback(Connection conn) {
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException re) {
            log.error("Rollback failed advancing checkpoint for pipeline '{}'", pipelineName, re);
        }
    }

    private void restoreAndClose(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            log.warn("Failed to restore auto-commit for pipeline '{}'", pipelineName, e);
        }
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Failed to close connection for pipeline '{}'", pipelineName, e);
        }
    }
}
