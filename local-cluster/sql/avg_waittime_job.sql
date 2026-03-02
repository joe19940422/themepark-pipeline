-- Create Kafka source table for raw themepark data
CREATE TABLE themepark_raw (
    entityId STRING,
    timestamp_ms BIGINT,
    status STRING,
    name STRING,
    waitTime INT,
    entityType STRING,
    event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'themepark_raw',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-themepark-analytics',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Create Kafka sink table for average wait times
CREATE TABLE attraction_avg_waittime (
    entityId STRING,
    name STRING,
    avg_waittime DOUBLE,
    PRIMARY KEY (entityId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'attraction_avg_waittime',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Calculate 20-minute sliding window (updates every 5 minutes)
INSERT INTO attraction_avg_waittime
SELECT
  entityId,
  REGEXP_REPLACE(name, '“|”|’','') AS name,
  AVG(CAST(waitTime AS DOUBLE)) AS avg_waittime
FROM TABLE(
  HOP(TABLE themepark_raw, DESCRIPTOR(event_time), INTERVAL '5' MINUTES, INTERVAL '20' MINUTES)
)
WHERE status = 'OPERATING'
GROUP BY entityId, REGEXP_REPLACE(name, '“|”|’', ''), window_start, window_end;
