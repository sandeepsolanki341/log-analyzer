package com.sandeep.pipeline.analyze;

import org.junit.jupiter.api.Test;

import com.sandeep.pipeline.analyze.Alert;
import com.sandeep.pipeline.analyze.AnomalyDetector;
import com.sandeep.pipeline.analyze.BaselineStore;
import com.sandeep.pipeline.analyze.InMemoryBaselineStore;
import com.sandeep.pipeline.analyze.WindowSnapshot;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AnomalyDetectorTest {

    private WindowSnapshot snap(long total, long errors,
                                Map<String, Long> bySvc, Map<String, Long> byIp) {
        return new WindowSnapshot(300, total, errors, bySvc, byIp, Map.of());
    }

    @Test
    void coldBaseline_doesNotFalseFire() {
        AnomalyDetector d = new AnomalyDetector(3.0, 50, 20, 0.3, new InMemoryBaselineStore());
        List<Alert> alerts = d.evaluate(snap(100, 40, Map.of(), Map.of()), Instant.now());
        assertFalse(alerts.stream().anyMatch(a -> a.rule().equals("ERROR_RATE_SPIKE")));
    }

    @Test
    void persistedBaseline_survivesRestartAndFiresSpike() {
        BaselineStore store = new InMemoryBaselineStore();
        AnomalyDetector d1 = new AnomalyDetector(3.0, 50, 20, 0.3, store);
        for (int i = 0; i < 10; i++) {
            d1.evaluate(snap(100, 5, Map.of(), Map.of()), Instant.now()); // ~5% baseline
        }
        // new detector instance, same durable store = "restart"
        AnomalyDetector d2 = new AnomalyDetector(3.0, 50, 20, 0.3, store);
        List<Alert> alerts = d2.evaluate(snap(100, 40, Map.of(), Map.of()), Instant.now());
        assertTrue(alerts.stream().anyMatch(a -> a.rule().equals("ERROR_RATE_SPIKE")));
    }

    @Test
    void failedLoginBurst_firesAtThreshold() {
        AnomalyDetector d = new AnomalyDetector(3.0, 50, 20, 0.3, new InMemoryBaselineStore());
        List<Alert> alerts = d.evaluate(snap(10, 0, Map.of(), Map.of("1.2.3.4", 25L)), Instant.now());
        assertTrue(alerts.stream()
                .anyMatch(a -> a.rule().equals("FAILED_LOGIN_BURST") && a.subject().equals("1.2.3.4")));
    }

    @Test
    void serviceErrorConcentration_fires() {
        AnomalyDetector d = new AnomalyDetector(3.0, 50, 20, 0.3, new InMemoryBaselineStore());
        List<Alert> alerts = d.evaluate(snap(200, 60, Map.of("checkout", 60L), Map.of()), Instant.now());
        assertTrue(alerts.stream()
                .anyMatch(a -> a.rule().equals("SERVICE_ERROR_CONCENTRATION") && a.subject().equals("checkout")));
    }
}
