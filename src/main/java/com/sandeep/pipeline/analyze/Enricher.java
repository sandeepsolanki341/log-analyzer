package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.parse.LogEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stage 5 — Enrichment (per-event, backed by bounded LRU caches). Joins each event against external
 * reference data (geo from IP, user segment, deployment version). Lookups are pluggable functions so
 * this class stays dependency-free and testable.
 *
 * <p>The LRU caches are wrapped in {@code synchronizedMap}, so this class is safe to share across
 * threads (though the pipeline drives it single-threaded).
 */
public class Enricher {

    @FunctionalInterface
    public interface GeoLookup {
        String resolve(String ip);
    }

    @FunctionalInterface
    public interface UserSegmentLookup {
        String resolve(long userId);
    }

    private final GeoLookup geoLookup;
    private final UserSegmentLookup userSegmentLookup;
    private final String deploymentVersion;
    private final Map<String, String> geoCache;
    private final Map<Long, String> userCache;

    public Enricher(GeoLookup geoLookup, UserSegmentLookup userSegmentLookup,
                    String deploymentVersion, int cacheCapacity) {
        this.geoLookup = geoLookup;
        this.userSegmentLookup = userSegmentLookup;
        this.deploymentVersion = deploymentVersion;
        this.geoCache = lru(cacheCapacity);
        this.userCache = lru(cacheCapacity);
    }

    public Map<String, String> enrich(LogEvent e) {
        Map<String, String> out = new HashMap<>();
        if (deploymentVersion != null) {
            out.put("deploymentVersion", deploymentVersion);
        }
        String ip = e.ip();
        if (ip != null && geoLookup != null) {
            String geo = geoCache.computeIfAbsent(ip, geoLookup::resolve);
            if (geo != null) {
                out.put("geo", geo);
            }
        }
        Long userId = e.userId();
        if (userId != null && userSegmentLookup != null) {
            String seg = userCache.computeIfAbsent(userId, userSegmentLookup::resolve);
            if (seg != null) {
                out.put("userSegment", seg);
            }
        }
        return out;
    }

    private static <K, V> Map<K, V> lru(int capacity) {
        int cap = Math.max(16, capacity);
        return Collections.synchronizedMap(new LinkedHashMap<>(cap, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > cap;
            }
        });
    }
}
