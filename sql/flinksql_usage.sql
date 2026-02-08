--
kafka message >>>
{"entityId": "d0e8c1f9-1fab-4081-bb7d-920815f28aa3", "timestamp_ms": 1770582064875, "status": "CLOSED", "name": "Seven Dwarfs Mine Train", "waitTime": 20, "entityType": "ATTRACTION"}
{"entityId": "57505d8a-1b5d-490e-82bd-1edf38505bfe", "timestamp_ms": 1770582064875, "status": "CLOSED", "name": "Dumbo the Flying Elephant", "waitTime": 30, "entityType": "ATTRACTION"}
{"entityId": "a1263174-68f3-4207-9c89-cb53860e1e0a", "timestamp_ms": 1770582064875, "status": "CLOSED", "name": "Hunny Pot Spin", "waitTime": 5, "entityType": "ATTRACTION"}
-- Create a database/namespace
CREATE DATABASE IF NOT EXISTS themepark_db;
USE themepark_db;

DROP TABLE IF EXISTS KafkaRideUpdates;
CREATE TABLE KafkaRideUpdates (
    -- Data fields directly mapped from your JSON payload (e.g., {"entityId": "...", "timestamp_ms": 1765139507616, ...})
    `entityId` STRING,
    `timestamp_ms` STRING,  -- Source field: Original event time in milliseconds since epoch
    `status` STRING,
    `name` STRING,
    `waitTime` INT,
    `entityType` STRING,

    -- VIRTUAL COLUMN: Convert the BIGINT timestamp (milliseconds) into a Flink TIMESTAMP_LTZ type.
    -- This is the column Flink will use for accurate event-time processing.
    `event_time` AS TO_TIMESTAMP_LTZ(CAST(`timestamp_ms` AS BIGINT), 3),

    -- WATERMARK: Define the Watermark based on the event_time.
    -- This allows for 5 seconds of potential delay or out-of-order arrival in the stream.
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'themepark-raw', -- Set this to your exact topic name
  'properties.bootstrap.servers' = '172.20.0.2:9092', -- Your Dockerized Kafka broker address
  -- Consumer Group ID: Define a unique, persistent name for this Flink job.
  'properties.group.id' = 'flink-themepark-wait-time-analytics',
  'scan.startup.mode' = 'earliest-offset', -- Useful for reading all existing data upon job start
  'format' = 'json',                      -- Specifies the input data format
  'json.timestamp-format.standard' = 'SQL', -- Recommended standard for Flink's time handling
  'json.fail-on-missing-field' = 'false', -- Prevents job failure if a field is occasionally missing
  'json.ignore-parse-errors' = 'true'     -- Skips records that cannot be parsed without crashing the job
);

1. Basic Data Inspection
Before doing math, verify that the timestamp conversion worked and that you are seeing the data.
SELECT
    name,
    status,
    waitTime,
    event_time
FROM KafkaRideUpdates
LIMIT 10;

2. Current Status Summary
Find out which rides are currently reporting the longest wait times.
SELECT *
FROM (
  SELECT
    window_start,
    window_end,
    name,
    waitTime,
    status,
    ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY waitTime DESC) as row_num
  FROM TABLE(
    TUMBLE(
      (SELECT * FROM KafkaRideUpdates WHERE status <> 'CLOSED'),
      DESCRIPTOR(event_time),
      INTERVAL '5' MINUTES
    )
  )
)
WHERE row_num <= 5;

3. Real-Time Rolling Statistics (Tumbling Window)
SELECT
    window_start,
    window_end,
    name,
    AVG(waitTime) as avg_wait,
    MAX(waitTime) as max_wait
FROM TABLE(
    TUMBLE(TABLE KafkaRideUpdates, DESCRIPTOR(event_time), INTERVAL '1' MINUTES))
GROUP BY window_start, window_end, name;