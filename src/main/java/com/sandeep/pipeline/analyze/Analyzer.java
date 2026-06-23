package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.parse.LogEvent;
import com.sandeep.pipeline.parse.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Layer 3 orchestrator. Chains the analysis stages over a batch of clean {@link LogEvent}s and emits
 * {@link AnalyzedEvent}s ready for the bulk indexer:
 *
 * <pre>
 *   classify, fingerprint, enrich, correlate, build doc      (per event)
 *   record into event-time window, evaluate detector, alerts  (cross event)
 * </pre>
 *
 * <h2>Event-time throughout</h2>
 * The window is fed and evaluated on event time (the aggregator's watermark), NOT wall-clock. Alerts
 * are stamped with that watermark, giving them a deterministic, replay-stable id and the correct
 * time-based index. This makes the entire analysis layer reproducible across restarts and replays.
 *
 * <h2>Deterministic ids</h2>
 * Real logs: {@code applog-<sourceId>}. Alerts: {@code alert-<rule>-<subject>-<epochSecond>}, so
 * identical alerts within the same second dedupe on replay rather than multiplying.
 *
 * <p>Threading: per-event stages are stateless, but the aggregator/detector are stateful, so a
 * single {@code Analyzer} runs on one pipeline thread (the single-writer assumption).
 */
public class Analyzer {

    private static final Logger log = LoggerFactory.getLogger(Analyzer.class);
    private static final DateTimeFormatter INDEX_DATE =
            DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
    private static final String INDEX_PREFIX = "app-logs-";
    private static final String ALERT_INDEX_PREFIX = "app-alerts-";

    private final Classifier classifier;
    private final Fingerprinter fingerprinter;
    private final SlidingWindowAggregator aggregator;
    private final AnomalyDetector detector;
    private final Enricher enricher;
    private final Correlator correlator;

    public Analyzer(Classifier classifier, Fingerprinter fingerprinter,
                    SlidingWindowAggregator aggregator, AnomalyDetector detector,
                    Enricher enricher, Correlator correlator) {
        this.classifier = classifier;
        this.fingerprinter = fingerprinter;
        this.aggregator = aggregator;
        this.detector = detector;
        this.enricher = enricher;
        this.correlator = correlator;
    }

    public List<AnalyzedEvent> analyzeBatch(List<LogEvent> events) {
        List<AnalyzedEvent> out = new ArrayList<>();
        if (events == null || events.isEmpty()) {
            return out;
        }

        for (LogEvent event : events) {
            Classification classification = classifier.classify(event);
            String fingerprint = fingerprinter.fingerprint(event);
            Map<String, String> enrichment = enricher.enrich(event);
            String traceId = correlator.correlate(event);

            aggregator.record(event, classification);

            out.add(new AnalyzedEvent(
                    event, classification, fingerprint, enrichment, traceId,
                    esIdFor(event.sourceId()),
                    indexFor(event.timestamp(), INDEX_PREFIX),
                    false));
        }

        // Cross-event detection over the event-time window. Alerts are stamped with the watermark
        // (max event time seen), making their timestamp and id deterministic across replays.
        Instant watermark = aggregator.watermark();
        WindowSnapshot snapshot = aggregator.snapshot();
        List<Alert> alerts = detector.evaluate(snapshot, watermark);
        for (Alert alert : alerts) {
            out.add(toSyntheticEvent(alert));
        }

        if (!alerts.isEmpty() || log.isDebugEnabled()) {
            log.info("Analyzed batch: {} event doc(s) + {} alert doc(s); window total={}, errorRate={}",
                    events.size(), alerts.size(), snapshot.totalEvents(),
                    String.format("%.2f%%", snapshot.errorRate() * 100));
        }
        return out;
    }

    private String esIdFor(long sourceId) {
        return "applog-" + sourceId;
    }

    private String indexFor(Instant ts, String prefix) {
        return prefix + INDEX_DATE.format(ts);
    }

    /** Turns an alert into a synthetic AnalyzedEvent in the alerts index, via named construction. */
    private AnalyzedEvent toSyntheticEvent(Alert alert) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("rule", alert.rule());
        fields.put("subject", alert.subject());

        LogEvent synthetic = new LogEvent(
                -1L,
                alert.at(),
                alert.severity() == SeverityBucket.CRITICAL ? LogLevel.ERROR : LogLevel.WARN,
                "analyzer",
                alert.message(),
                null, null, null, null, null,
                fields);

        Classification cls = new Classification("ALERT", "OBSERVABILITY", alert.severity());
        String id = "alert-" + alert.rule() + "-" + alert.subject() + "-" + alert.at().getEpochSecond();

        return new AnalyzedEvent(
                synthetic, cls, "alert:" + alert.rule(),
                Map.of(), null, id,
                indexFor(alert.at(), ALERT_INDEX_PREFIX), true);
    }
}
