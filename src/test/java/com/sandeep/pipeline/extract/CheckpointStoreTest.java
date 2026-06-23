package com.sandeep.pipeline.extract;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sandeep.pipeline.extract.CheckpointStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Uses H2 in MySQL-compatibility mode to exercise the real SQL, including the GREATEST-based
 * monotonic UPSERT.
 */
class CheckpointStoreTest {

    private Connection keepAlive;
    private DataSource ds;

    @BeforeEach
    void setUp() throws Exception {
        String url = "jdbc:h2:mem:cp_" + System.nanoTime() + ";MODE=MySQL;DB_CLOSE_DELAY=-1";
        keepAlive = DriverManager.getConnection(url);
        try (Statement st = keepAlive.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS pipeline_state ("
                    + "pipeline_name VARCHAR(128) NOT NULL,"
                    + "last_processed_id BIGINT NOT NULL DEFAULT 0,"
                    + "PRIMARY KEY (pipeline_name))");
        }
        ds = new SingleConnectionDataSource(url);
    }

    @AfterEach
    void tearDown() throws Exception {
        keepAlive.close();
    }

    @Test
    void readReturnsZeroWhenNoRow() {
        CheckpointStore cs = new CheckpointStore(ds, "p1");
        assertEquals(0L, cs.read());
    }

    @Test
    void advanceThenReadRoundTrips() {
        CheckpointStore cs = new CheckpointStore(ds, "p1");
        cs.advanceTo(100);
        assertEquals(100L, cs.read());
    }

    @Test
    void advanceIsMonotonic_neverRewinds() {
        CheckpointStore cs = new CheckpointStore(ds, "p1");
        cs.advanceTo(100);
        cs.advanceTo(50); // stale/out-of-order call
        assertEquals(100L, cs.read(), "GREATEST must prevent rewind");
        cs.advanceTo(150);
        assertEquals(150L, cs.read());
    }

    /** Minimal DataSource over a fresh connection per call (H2 in-mem shared via DB_CLOSE_DELAY). */
    static final class SingleConnectionDataSource implements DataSource {
        private final String url;
        SingleConnectionDataSource(String url) { this.url = url; }
        public Connection getConnection() throws java.sql.SQLException { return DriverManager.getConnection(url); }
        public Connection getConnection(String u, String p) throws java.sql.SQLException { return getConnection(); }
        public java.io.PrintWriter getLogWriter() { return null; }
        public void setLogWriter(java.io.PrintWriter out) { }
        public void setLoginTimeout(int seconds) { }
        public int getLoginTimeout() { return 0; }
        public java.util.logging.Logger getParentLogger() { return null; }
        public <T> T unwrap(Class<T> iface) { return null; }
        public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
