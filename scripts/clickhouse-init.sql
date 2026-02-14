-- Initialize ClickHouse database for analytics

-- Create analytics database
CREATE DATABASE IF NOT EXISTS db_stream_analytics;

-- Use the analytics database
USE db_stream_analytics;

-- Create table for user analytics
CREATE TABLE IF NOT EXISTS user_events (
    id String,
    user_id UInt64,
    email String,
    event_type String,
    table_name String,
    operation String,
    event_time DateTime64(3) DEFAULT now64(),
    data String,
    old_data String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (user_id, event_time, event_type)
TTL event_time + INTERVAL 30 DAY;

-- Create table for product analytics
CREATE TABLE IF NOT EXISTS product_events (
    id String,
    product_id UInt64,
    name String,
    price Decimal(10, 2),
    category_id UInt64,
    event_type String,
    table_name String,
    operation String,
    event_time DateTime64(3) DEFAULT now64(),
    data String,
    old_data String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (product_id, event_time, event_type)
TTL event_time + INTERVAL 30 DAY;

-- Create table for order analytics
CREATE TABLE IF NOT EXISTS order_events (
    id String,
    order_id UInt64,
    user_id UInt64,
    total_amount Decimal(10, 2),
    status String,
    event_type String,
    table_name String,
    operation String,
    event_time DateTime64(3) DEFAULT now64(),
    data String,
    old_data String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (order_id, event_time, event_type)
TTL event_time + INTERVAL 30 DAY;

-- Create aggregated tables for faster queries
CREATE TABLE IF NOT EXISTS user_activity_summary (
    user_id UInt64,
    email String,
    total_events UInt64,
    last_activity DateTime64(3),
    created_at DateTime64(3) DEFAULT now64(),
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS product_stats (
    product_id UInt64,
    name String,
    category_id UInt64,
    total_events UInt64,
    avg_price Decimal(10, 2),
    last_updated DateTime64(3),
    created_at DateTime64(3) DEFAULT now64(),
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS order_stats (
    date Date,
    total_orders UInt64,
    total_amount Decimal(15, 2),
    avg_order_value Decimal(10, 2),
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(date)
ORDER BY date;

-- Create materialized view for real-time aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS user_activity_mv
TO user_activity_summary
AS SELECT
    user_id,
    any(email) as email,
    count() as total_events,
    max(event_time) as last_activity,
    now64() as created_at,
    now64() as updated_at
FROM user_events
GROUP BY user_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS product_stats_mv
TO product_stats
AS SELECT
    product_id,
    any(name) as name,
    any(category_id) as category_id,
    count() as total_events,
    avg(price) as avg_price,
    max(event_time) as last_updated,
    now64() as created_at,
    now64() as updated_at
FROM product_events
WHERE operation != 'DELETE'
GROUP BY product_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS order_stats_mv
TO order_stats
AS SELECT
    toDate(event_time) as date,
    count() as total_orders,
    sum(total_amount) as total_amount,
    avg(total_amount) as avg_order_value,
    now64() as created_at
FROM order_events
WHERE operation != 'DELETE'
GROUP BY toDate(event_time);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events (user_id) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_user_events_type ON user_events (event_type) TYPE tokenbf_v1(3, 0) GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_product_events_product_id ON product_events (product_id) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_order_events_user_id ON order_events (user_id) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_order_events_status ON order_events (status) TYPE tokenbf_v1(3, 0) GRANULARITY 1;

-- Grant permissions for the application user
CREATE USER IF NOT EXISTS 'db_stream' IDENTIFIED BY '';
GRANT SELECT, INSERT ON db_stream_analytics.* TO 'db_stream';
