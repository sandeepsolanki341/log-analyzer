package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.parse.LogEvent;
import com.sandeep.pipeline.parse.LogLevel;

/**
 * Stage 1 — Classification (per-event, stateless, thread-safe). Derives categorical tags from an
 * event's attributes (status, exception, service, level) so logs are filterable/groupable downstream
 * in ways the raw level alone cannot support.
 */
public class Classifier {

    public Classification classify(LogEvent e) {
        return new Classification(deriveCategory(e), deriveDomain(e), deriveSeverity(e));
    }

    private String deriveCategory(LogEvent e) {
        Integer status = e.statusCode();
        if (status != null) {
            if (status == 401 || status == 403) {
                return "AUTH";
            }
            if (status == 429) {
                return "RATE_LIMIT";
            }
            if (status >= 500) {
                return "SERVER_ERROR";
            }
            if (status >= 400) {
                return "CLIENT_ERROR";
            }
        }
        String exception = e.extractedFields().get("exception");
        if (exception != null) {
            String ex = exception.toLowerCase();
            if (ex.contains("sql") || ex.contains("dataaccess") || ex.contains("jdbc")) {
                return "DATABASE";
            }
            if (ex.contains("timeout") || ex.contains("connect") || ex.contains("socket")) {
                return "NETWORK";
            }
            if (ex.contains("nullpointer") || ex.contains("illegalstate") || ex.contains("illegalargument")) {
                return "APPLICATION_BUG";
            }
            return "EXCEPTION";
        }
        return "GENERAL";
    }

    private String deriveDomain(LogEvent e) {
        String svc = e.service();
        if (svc == null) {
            return "UNKNOWN";
        }
        String s = svc.toLowerCase();
        if (s.contains("checkout") || s.contains("payment") || s.contains("order")) {
            return "PAYMENTS";
        }
        if (s.contains("auth") || s.contains("login") || s.contains("account") || s.contains("user")) {
            return "ACCOUNT";
        }
        if (s.contains("catalog") || s.contains("product") || s.contains("search")) {
            return "CATALOG";
        }
        return svc;
    }

    private SeverityBucket deriveSeverity(LogEvent e) {
        LogLevel level = e.level();
        if (level == LogLevel.FATAL || level == LogLevel.ERROR) {
            return SeverityBucket.CRITICAL;
        }
        Integer status = e.statusCode();
        if (status != null && status >= 500) {
            return SeverityBucket.CRITICAL;
        }
        if (level == LogLevel.WARN || (status != null && status >= 400)) {
            return SeverityBucket.DEGRADED;
        }
        return SeverityBucket.NORMAL;
    }
}
