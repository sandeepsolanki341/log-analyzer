-- =====================================================================
-- HealthSpan Log Analysis Pipeline — MySQL schema
-- =====================================================================
-- Apply with:  mysql -u root -p logs < deploy/sql/schema.sql
-- IMPORTANT (timestamps): app_logs.ts is DATETIME (zoneless). The pipeline
-- interprets it in SOURCE_ZONE (default UTC). If your services write a
-- TIMESTAMP column instead (which MySQL stores as UTC and the driver converts
-- to the session zone), pin the JDBC session to UTC via the connection string
-- (connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true) and keep
-- SOURCE_ZONE=UTC. Mismatching these shifts every event by the zone offset.

CREATE TABLE IF NOT EXISTS app_logs (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    ts          DATETIME(3)  NOT NULL,
    level       VARCHAR(16),
    service     VARCHAR(128),
    message     TEXT,
    user_id     BIGINT,
    ip          VARCHAR(64),
    status_code INT,
    latency_ms  BIGINT,
    stack_trace MEDIUMTEXT,
    PRIMARY KEY (id)
    -- id is the PK, which is exactly the index keyset pagination seeks on.
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Durable extraction checkpoint (high-water mark per pipeline).
CREATE TABLE IF NOT EXISTS pipeline_state (
    pipeline_name     VARCHAR(128) NOT NULL,
    last_processed_id BIGINT       NOT NULL DEFAULT 0,
    updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                   ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Quarantine for rows that could not be parsed (poison-pill isolation).
CREATE TABLE IF NOT EXISTS parse_dead_letter (
    source_id   BIGINT       NOT NULL,
    raw_message TEXT,
    reason      VARCHAR(512) NOT NULL,
    failed_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Quarantine for documents Elasticsearch rejected permanently / retry-exhausted.
CREATE TABLE IF NOT EXISTS index_dead_letter (
    es_id        VARCHAR(512) NOT NULL,
    target_index VARCHAR(255),
    http_status  INT,
    reason       VARCHAR(1024),
    body         MEDIUMTEXT,
    failed_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (es_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
