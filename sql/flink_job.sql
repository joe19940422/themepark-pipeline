-- Set execution properties for a streaming job
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.source.idle-timeout' = '10s';

-- 1. Create the Iceberg Catalog (The Metadata Store)
CREATE CATALOG iceberg_catalog WITH (
    'type'='iceberg',
    'catalog-type'='hadoop',
    'warehouse'='s3a://iceberg-warehouse/',
    'hadoop-conf.fs.s3a.endpoint'='http://minio:9000',
    'hadoop-conf.fs.s3a.access.key'='minioadmin',
    'hadoop-conf.fs.s3a.secret.key'='minioadmin',
    'hadoop-conf.fs.s3a.path.style.access'='true',
    'hadoop-conf.fs.s3a.connection.timeout'='300000'
);

USE CATALOG iceberg_catalog;

-- Create a database/namespace
CREATE DATABASE IF NOT EXISTS themepark_db;
USE themepark_db;

-- 2. Define the Kafka Source Table
CREATE TABLE raw_rides (
    entityId STRING,
    name STRING,
    status STRING,
    waitTime INT,
    event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'themepark-raw',
    'properties.bootstrap.servers' = 'kafka:9092', -- KRaft connection remains the same
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'scan.startup.mode' = 'latest-offset'
);

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
select count(*) from KafkaRideUpdates;
select count(*), status from KafkaRideUpdates group by status limit 5;
select * from KafkaRideUpdates where timestamp_ms in (select max(timestamp_ms) from KafkaRideUpdates) and status ='OPERATING'
order by waitTime desc limit 50;


-- 3. Define the Iceberg Sink Table
CREATE TABLE ranked_wait_times (
    window_end_time TIMESTAMP_LTZ(3),
    ride_rank INT,
    entityId STRING,
    name STRING,
    waitTime INT,
    status STRING
) PARTITIONED BY (window_end_time)
WITH (
    'write.upsert.enabled'='true',
    'write.distribution-mode'='hash'
);

-- 4. Execute the Continuous Streaming Job (Filter and Rank)
INSERT INTO ranked_wait_times
SELECT
    window_end,
    ROW_NUMBER() OVER (
        PARTITION BY window_start
        ORDER BY waitTime DESC
    ) AS ride_rank,
    entityId,
    name,
    waitTime,
    status
FROM TABLE(
    TUMBLE(TABLE raw_rides, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE status = 'OPERATING'
  AND waitTime IS NOT NULL
  AND waitTime > 0;