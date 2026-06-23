package com.sandeep.pipeline.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigTest {

    @Test
    void loadsDefaultsAndValidates() {
        PipelineConfig cfg = PipelineConfig.load(new Properties());
        assertEquals("app-logs", cfg.pipelineName);
        assertEquals(5000, cfg.batchSize);
        assertEquals(300, cfg.window.toSeconds());
        assertTrue(cfg.indexMaxBytesPerRequest > 0);
    }

    @Test
    void propertiesOverrideDefaults() {
        Properties p = new Properties();
        p.setProperty("batch.size", "1234");
        p.setProperty("window.seconds", "60");
        PipelineConfig cfg = PipelineConfig.load(p);
        assertEquals(1234, cfg.batchSize);
        assertEquals(60, cfg.window.toSeconds());
    }

    @Test
    void invalidConfigRejected() {
        Properties p = new Properties();
        p.setProperty("batch.size", "0");
        assertThrows(IllegalStateException.class, () -> PipelineConfig.load(p));
    }

    @Test
    void invalidEmaAlphaRejected() {
        Properties p = new Properties();
        p.setProperty("anomaly.ema.alpha", "1.5");
        assertThrows(IllegalStateException.class, () -> PipelineConfig.load(p));
    }
}
