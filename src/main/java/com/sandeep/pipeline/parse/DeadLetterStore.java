package com.sandeep.pipeline.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Durable sink for rows that failed parsing. Idempotent on {@code source_id} via
 * {@code ON DUPLICATE KEY UPDATE} so replays don't violate the primary key.
 *
 * <pre>{@code
 *   CREATE TABLE parse_dead_letter (
 *       source_id   BIGINT       NOT NULL,
 *       raw_message TEXT,
 *       reason      VARCHAR(512) NOT NULL,
 *       failed_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
 *       PRIMARY KEY (source_id)
 *   );
 * }</pre>
 */
public class DeadLetterStore {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterStore.class);

    private static final String INSERT_SQL =
            "INSERT INTO parse_dead_letter (source_id, raw_message, reason) VALUES (?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE raw_message = VALUES(raw_message), reason = VALUES(reason)";

    private final DataSource dataSource;

    public DeadLetterStore(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    public void persist(List<DeadLetterRecord> deadLetters) {
        if (deadLetters == null || deadLetters.isEmpty()) {
            return;
        }
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                for (DeadLetterRecord dl : deadLetters) {
                    ps.setLong(1, dl.raw().id());
                    ps.setString(2, dl.raw().message());
                    ps.setString(3, truncate(dl.reason(), 512));
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                safeRollback(conn);
                throw e;
            }
            log.warn("Quarantined {} unparseable row(s) to parse dead-letter store", deadLetters.size());
        } catch (SQLException e) {
            log.error("Failed to persist {} dead-letter record(s)", deadLetters.size(), e);
            throw new DeadLetterPersistenceException("Could not persist dead-letter batch", e);
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
            log.error("Rollback failed persisting dead letters", re);
        }
    }

    private void restoreAndClose(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            log.warn("Failed to restore auto-commit on dead-letter connection", e);
        }
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Failed to close dead-letter connection", e);
        }
    }

    public static class DeadLetterPersistenceException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public DeadLetterPersistenceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
