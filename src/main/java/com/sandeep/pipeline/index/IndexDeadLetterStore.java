package com.sandeep.pipeline.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Durable quarantine for documents Elasticsearch rejected permanently or that exhausted retries.
 * Mirrors the parser's dead-letter pattern. Idempotent on {@code es_id}.
 *
 * <pre>{@code
 *   CREATE TABLE index_dead_letter (
 *       es_id        VARCHAR(512) NOT NULL,
 *       target_index VARCHAR(255),
 *       http_status  INT,
 *       reason       VARCHAR(1024),
 *       body         MEDIUMTEXT,
 *       failed_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 *       PRIMARY KEY (es_id)
 *   );
 * }</pre>
 */
public class IndexDeadLetterStore {

    private static final Logger log = LoggerFactory.getLogger(IndexDeadLetterStore.class);

    private static final String INSERT_SQL =
            "INSERT INTO index_dead_letter (es_id, target_index, http_status, reason, body) "
                    + "VALUES (?, ?, ?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE http_status = VALUES(http_status), "
                    + "reason = VALUES(reason), body = VALUES(body)";

    private final DataSource dataSource;

    public IndexDeadLetterStore(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    public void persist(List<BulkResponse.ItemFailure> failures) {
        if (failures == null || failures.isEmpty()) {
            return;
        }
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                for (BulkResponse.ItemFailure f : failures) {
                    IndexOperation op = f.operation();
                    ps.setString(1, op.id());
                    ps.setString(2, op.index());
                    ps.setInt(3, f.httpStatus());
                    ps.setString(4, truncate(f.reason(), 1024));
                    ps.setString(5, op.json());
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                safeRollback(conn);
                throw e;
            }
            log.warn("Quarantined {} document(s) to index dead-letter store", failures.size());
        } catch (SQLException e) {
            log.error("Failed to persist {} index dead-letter(s)", failures.size(), e);
            throw new IndexDeadLetterPersistenceException("Could not persist index dead-letter batch", e);
        } finally {
            restoreAndClose(conn);
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() <= max ? s : s.substring(0, max);
    }

    private void safeRollback(Connection conn) {
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException re) {
            log.error("Rollback failed persisting index dead-letters", re);
        }
    }

    private void restoreAndClose(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            log.warn("Failed to restore auto-commit on index dead-letter connection", e);
        }
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Failed to close index dead-letter connection", e);
        }
    }

    public static class IndexDeadLetterPersistenceException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public IndexDeadLetterPersistenceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
