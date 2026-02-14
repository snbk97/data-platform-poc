SET allow_experimental_s3queue = 1;

-- ---------------------------------------------------------------------------
-- 1. Destination table (ReplacingMergeTree) – deduplicates by show_id,
--    keeps latest row per key (update semantics). Version column _ingested_at
--    ensures the most recently ingested row wins when the same show_id is
--    re-ingested (e.g. updated CSV). Use SELECT ... FINAL or OPTIMIZE TABLE
--    ... FINAL for fully deduplicated reads.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS db_stream_analytics.netflix_titles
(
    show_id       UInt64,
    `type`        LowCardinality(String),
    title         String,
    director      String,
    `cast`        String,
    country       String,
    date_added    String,
    release_year  UInt16,
    rating        LowCardinality(String),
    duration      String,
    listed_in     String,
    description   String,
    _ingested_at  DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY release_year
ORDER BY (show_id, release_year)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- 2. S3Queue source table – polls clickhouse-360-1 bucket for *.csv files
--    CSVWithNames format auto-skips the header row and maps by column name
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS db_stream_analytics.netflix_titles_s3_queue
(
    show_id       UInt64,
    `type`        LowCardinality(String),
    title         String,
    director      String,
    `cast`        String,
    country       String,
    date_added    String,
    release_year  UInt16,
    rating        LowCardinality(String),
    duration      String,
    listed_in     String,
    description   String
) ENGINE = S3Queue(
    'https://clickhouse-360-1.s3.ap-south-1.amazonaws.com/*.csv',
    'CSVWithNames'
)
SETTINGS
    mode                           = 'unordered',
    s3queue_polling_min_timeout_ms = 1000,   -- 1 minutes
    s3queue_polling_max_timeout_ms = 3600000;  -- 1 hour

-- ---------------------------------------------------------------------------
-- 3. Materialized view – bridges S3Queue → MergeTree (per-bucket view)
--    One MV per bucket keeps ingestion pipelines isolated and observable
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS db_stream_analytics.netflix_titles_mv_clickhouse_360_1
TO db_stream_analytics.netflix_titles
AS
SELECT
    show_id,
    `type`,
    title,
    director,
    `cast`,
    country,
    date_added,
    release_year,
    rating,
    duration,
    listed_in,
    description,
    now64() AS _ingested_at
FROM db_stream_analytics.netflix_titles_s3_queue;
