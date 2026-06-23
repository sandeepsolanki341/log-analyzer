-- Optional sample data for a quick local smoke test.
INSERT INTO app_logs (ts, level, service, message, user_id, ip, status_code, latency_ms, stack_trace) VALUES
  (UTC_TIMESTAMP(), 'INFO',  'checkout-svc', 'order placed user=8821 traceId=abc-123', 8821, '203.0.113.7', 200, 45, NULL),
  (UTC_TIMESTAMP(), 'ERROR', 'checkout-svc', 'payment failed', 8821, '203.0.113.7', 503, 1200,
     'java.lang.NullPointerException\n\tat com.app.Pay.charge(Pay.java:88)'),
  (UTC_TIMESTAMP(), 'WARN',  'auth-svc', 'invalid credentials', NULL, '198.51.100.23', 401, 12, NULL),
  (UTC_TIMESTAMP(), 'INFO',  'catalog-svc', '{"level":"info","service":"catalog-svc","msg":"search ok","latencyMs":30,"context":{"q":"vitamin d"}}', NULL, NULL, 200, 30, NULL);
