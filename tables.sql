-- ====================================================================
-- 1. DIMENSION TABLES (Must be created first for Foreign Keys)
-- ====================================================================

CREATE TABLE symbols (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    sector VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE
);

-- Insert the 10 symbols used in your Python script
INSERT INTO symbols (symbol, company_name, sector) VALUES 
    ('GOOGL', 'Alphabet Inc.', 'Technology'),
    ('META', 'Meta Platforms', 'Technology'),
    ('TSLA', 'Tesla, Inc.', 'Consumer Discretionary'),
    ('NVDA', 'NVIDIA Corporation', 'Technology'),
    ('AMZN', 'Amazon.com', 'Consumer Discretionary'),
    ('NFLX', 'Netflix, Inc.', 'Communication Services'),
    ('MSFT', 'Microsoft Corp.', 'Technology'),
    ('AAPL', 'Apple Inc.', 'Technology'),
    ('TSMC', 'Taiwan Semiconductor', 'Technology'),
    ('INTC', 'Intel Corporation', 'Technology')
ON CONFLICT (symbol) DO NOTHING;

-- ====================================================================
-- 2. CORE TIME-SERIES TABLES (The tick and aggregation data)
-- ====================================================================

-- Raw Ticks (Partitioned by time)
CREATE TABLE raw_ticks (
    ts TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
    price NUMERIC(10, 2) NOT NULL,
    volume INTEGER NOT NULL
) PARTITION BY RANGE (ts);

-- Create a default partition to catch data before you set up your daily automated triggers
CREATE TABLE raw_ticks_default PARTITION OF raw_ticks DEFAULT;

-- 1-Second Aggregations
CREATE TABLE ohlcv_1s (
    bucket_time TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    total_volume INTEGER,
    PRIMARY KEY (bucket_time, symbol)
);

-- 1-Minute Aggregations (Now Partitioned)
CREATE TABLE ohlcv_1m (
    bucket_time TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    total_volume INTEGER,
    -- The primary key MUST include the partition key (bucket_time)
    PRIMARY KEY (bucket_time, symbol) 
) PARTITION BY RANGE (bucket_time);

-- Create a default partition to catch initial data
CREATE TABLE ohlcv_1m_default PARTITION OF ohlcv_1m DEFAULT;

-- Performance Index
CREATE INDEX idx_ohlcv_1m_time ON ohlcv_1m (bucket_time DESC);
-- ====================================================================
-- 3. OPERATIONAL & STATE TABLES (For background processes and logging)
-- ====================================================================

-- Tracks where the continuous aggregation left off
CREATE TABLE aggregation_watermarks (
    aggregation_interval VARCHAR(10) PRIMARY KEY, 
    last_processed_ts TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Initialize the watermarks (Assuming your simulation starts today)
INSERT INTO aggregation_watermarks (aggregation_interval, last_processed_ts) VALUES 
    ('1s', '2020-01-01 00:00:00'),
    ('1m', '2020-01-01 00:00:00')
ON CONFLICT DO NOTHING;

-- Logs execution times for your DBMS Performance Dashboard
CREATE TABLE query_metrics (
    id SERIAL PRIMARY KEY,
    query_description VARCHAR(100) NOT NULL,
    target_table VARCHAR(50) NOT NULL,
    execution_time_ms NUMERIC(10, 3) NOT NULL,
    rows_scanned INTEGER,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- 4. PERFORMANCE INDEXING
-- ====================================================================

-- Critical for speeding up time-range queries and OHLCV aggregations
CREATE INDEX idx_raw_ticks_symbol_ts ON raw_ticks (symbol, ts DESC);
CREATE INDEX idx_ohlcv_1s_time ON ohlcv_1s (bucket_time DESC);
CREATE INDEX idx_ohlcv_1m_time ON ohlcv_1m (bucket_time DESC);