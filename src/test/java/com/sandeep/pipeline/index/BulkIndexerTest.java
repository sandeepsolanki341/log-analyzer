package com.sandeep.pipeline.index;

import com.sandeep.pipeline.analyze.AnalyzedEvent;
import com.sandeep.pipeline.analyze.Classification;
import com.sandeep.pipeline.analyze.SeverityBucket;
import com.sandeep.pipeline.parse.LogEvent;
import com.sandeep.pipeline.parse.LogLevel;
import com.sandeep.pipeline.util.Metrics;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class BulkIndexerTest {

    // ---- test doubles ----

    static final class FakeTransport implements ElasticsearchTransport {
        final List<Integer> requestSizes = new ArrayList<>();
        int callCount = 0;
        final Function<List<IndexOperation>, BulkResponse> behavior;
        boolean throwTransport = false;

        FakeTransport(Function<List<IndexOperation>, BulkResponse> behavior) {
            this.behavior = behavior;
        }

        @Override
        public BulkResponse bulk(List<IndexOperation> ops) throws TransportException {
            callCount++;
            if (throwTransport) {
                throw new TransportException("connection refused");
            }
            requestSizes.add(ops.size());
            return behavior.apply(ops);
        }
    }

    /** Capturing dead-letter store that bypasses JDBC. */
    static final class CapturingDeadLetters extends IndexDeadLetterStore {
        final List<BulkResponse.ItemFailure> captured = new ArrayList<>();
        CapturingDeadLetters() {
            super(noopDataSource());
        }
        @Override
        public void persist(List<BulkResponse.ItemFailure> failures) {
            captured.addAll(failures);
        }
    }

    private static DataSource noopDataSource() {
        // Never used (persist is overridden), but the ctor requires non-null.
        return (DataSource) java.lang.reflect.Proxy.newProxyInstance(
                BulkIndexerTest.class.getClassLoader(),
                new Class[]{DataSource.class},
                (p, m, a) -> {
                    if (m.getName().equals("getConnection")) {
                        return (Connection) java.lang.reflect.Proxy.newProxyInstance(
                                BulkIndexerTest.class.getClassLoader(),
                                new Class[]{Connection.class}, (p2, m2, a2) -> null);
                    }
                    return null;
                });
    }

    private AnalyzedEvent ae(long id) {
        LogEvent e = new LogEvent(id, Instant.parse("2026-01-01T00:00:00Z"), LogLevel.INFO,
                "svc", "m", null, null, null, null, null, Map.of());
        return new AnalyzedEvent(e, new Classification("GENERAL", "svc", SeverityBucket.NORMAL),
                "fp", Map.of(), null, "applog-" + id, "app-logs-2026.01.01", false);
    }

    private List<AnalyzedEvent> batch(int n) {
        List<AnalyzedEvent> b = new ArrayList<>();
        for (long i = 1; i <= n; i++) {
            b.add(ae(i));
        }
        return b;
    }

    private final Metrics metrics = new Metrics();

    @Test
    void subBatchesByDocumentCount() {
        FakeTransport ft = new FakeTransport(ops -> new BulkResponse(ops.size(), List.of()));
        BulkIndexer idx = new BulkIndexer(ft, new CapturingDeadLetters(),
                new BackoffPolicy(3, 1, 2), 500, 100_000_000L, metrics);
        int n = idx.indexBatch(batch(1000));
        assertEquals(List.of(500, 500), ft.requestSizes);
        assertEquals(1000, n);
    }

    @Test
    void subBatchesByByteSize() {
        FakeTransport ft = new FakeTransport(ops -> new BulkResponse(ops.size(), List.of()));
        BulkIndexer idx = new BulkIndexer(ft, new CapturingDeadLetters(),
                new BackoffPolicy(3, 1, 2), 9999, 20L, metrics);
        idx.indexBatch(batch(5));
        assertEquals(5, ft.callCount); // tiny byte cap forces ~1 doc/request
    }

    @Test
    void transientFailureRetriedThenSucceeds() {
        int[] call = {0};
        FakeTransport ft = new FakeTransport(ops -> {
            call[0]++;
            if (call[0] == 1) {
                return new BulkResponse(ops.size(),
                        ops.stream().map(o -> new BulkResponse.ItemFailure(o, 429, "busy")).toList());
            }
            return new BulkResponse(ops.size(), List.of());
        });
        CapturingDeadLetters dl = new CapturingDeadLetters();
        BulkIndexer idx = new BulkIndexer(ft, dl, new BackoffPolicy(4, 1, 2), 500, 1_000_000L, metrics);
        assertEquals(10, idx.indexBatch(batch(10)));
        assertTrue(dl.captured.isEmpty());
    }

    @Test
    void permanentFailureDeadLetteredNotRetried() {
        FakeTransport ft = new FakeTransport(ops ->
                new BulkResponse(ops.size(),
                        List.of(new BulkResponse.ItemFailure(ops.get(0), 400, "mapping conflict"))));
        CapturingDeadLetters dl = new CapturingDeadLetters();
        BulkIndexer idx = new BulkIndexer(ft, dl, new BackoffPolicy(4, 1, 2), 500, 1_000_000L, metrics);
        int n = idx.indexBatch(batch(3));
        assertEquals(1, dl.captured.size());
        assertEquals(2, n);
        assertEquals(1, ft.callCount);
    }

    @Test
    void retryExhaustionDeadLetters() {
        FakeTransport ft = new FakeTransport(ops ->
                new BulkResponse(ops.size(),
                        ops.stream().map(o -> new BulkResponse.ItemFailure(o, 503, "unavailable")).toList()));
        CapturingDeadLetters dl = new CapturingDeadLetters();
        BulkIndexer idx = new BulkIndexer(ft, dl, new BackoffPolicy(3, 1, 2), 500, 1_000_000L, metrics);
        assertEquals(0, idx.indexBatch(batch(4)));
        assertEquals(4, dl.captured.size());
    }

    @Test
    void transportDownExhaustedThrows() {
        FakeTransport ft = new FakeTransport(ops -> new BulkResponse(ops.size(), List.of()));
        ft.throwTransport = true;
        BulkIndexer idx = new BulkIndexer(ft, new CapturingDeadLetters(),
                new BackoffPolicy(2, 1, 2), 500, 1_000_000L, metrics);
        assertThrows(BulkIndexException.class, () -> idx.indexBatch(batch(3)));
    }

    @Test
    void interruptDuringIndexingThrowsPromptly() {
        FakeTransport ft = new FakeTransport(ops -> {
            Thread.currentThread().interrupt();
            return new BulkResponse(ops.size(),
                    ops.stream().map(o -> new BulkResponse.ItemFailure(o, 503, "busy")).toList());
        });
        BulkIndexer idx = new BulkIndexer(ft, new CapturingDeadLetters(),
                new BackoffPolicy(5, 1000, 5000), 500, 1_000_000L, metrics);
        try {
            assertThrows(BulkIndexException.class, () -> idx.indexBatch(batch(3)));
        } finally {
            Thread.interrupted(); // clear flag
        }
    }
}
