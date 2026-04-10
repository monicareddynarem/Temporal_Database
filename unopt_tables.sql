-- ==========================================================
-- 1. DIMENSION TABLES
-- ==========================================================
CREATE TABLE symbols (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    sector VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE
);

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

-- ==========================================================
-- 2. CORE TABLES (PARTITIONED)
-- ==========================================================

-- RAW TICKS: Daily Partitions
CREATE TABLE raw_ticks (
    ts TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
    price NUMERIC(10, 2) NOT NULL,
    volume INTEGER NOT NULL
) PARTITION BY RANGE (ts);

-- 1-SECOND OHLCV: Daily Partitions
CREATE TABLE ohlcv_1s (
    ts_bucket TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    volume INTEGER,
    -- Note: PKs on partitioned tables must include the partition key (ts_bucket)
    PRIMARY KEY (ts_bucket, symbol)
) PARTITION BY RANGE (ts_bucket);

-- 1-MINUTE OHLCV: Daily Partitions
CREATE TABLE ohlcv_1m (
    ts_bucket TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    volume INTEGER,
    PRIMARY KEY (ts_bucket, symbol)
) PARTITION BY RANGE (ts_bucket);

-- ==========================================================
-- 3. THE MASTER AUTOMATION TRIGGER (Partitioning + Indexing)
-- ==========================================================

CREATE OR REPLACE FUNCTION manage_daily_partitions()
RETURNS TRIGGER AS $$
DECLARE
    p_name TEXT;
    p_start TEXT;
    p_end TEXT;
    target_table TEXT := TG_TABLE_NAME;
    ts_value TIMESTAMP;
BEGIN
    IF target_table = 'raw_ticks' THEN 
        ts_value := NEW.ts;
    ELSE 
        ts_value := NEW.ts_bucket;
    END IF;

    p_name := target_table || '_' || to_char(ts_value, 'YYYY_MM_DD');
    p_start := to_char(ts_value, 'YYYY-MM-DD 00:00:00');
    p_end := to_char(ts_value + INTERVAL '1 day', 'YYYY-MM-DD 00:00:00');

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE c.relname = p_name
    ) THEN
        
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
            p_name, target_table, p_start, p_end
        );

    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply Partition Trigger to ALL tables
CREATE TRIGGER trg_partition_raw BEFORE INSERT ON raw_ticks FOR EACH ROW EXECUTE FUNCTION manage_daily_partitions();
CREATE TRIGGER trg_partition_1s  BEFORE INSERT ON ohlcv_1s  FOR EACH ROW EXECUTE FUNCTION manage_daily_partitions();
CREATE TRIGGER trg_partition_1m  BEFORE INSERT ON ohlcv_1m  FOR EACH ROW EXECUTE FUNCTION manage_daily_partitions();

-- ==========================================================
-- 4. STATE MANAGEMENT (Watermarks)
-- ==========================================================

CREATE TABLE aggregation_watermarks (
    aggregation_interval VARCHAR(10) PRIMARY KEY, 
    last_processed_ts TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Init watermarks (Starting point)
INSERT INTO aggregation_watermarks (aggregation_interval, last_processed_ts) 
VALUES ('1s', '2026-04-09 00:00:00'), ('1m', '2026-04-09 00:00:00')
ON CONFLICT DO NOTHING;

-- ==========================================================
-- 5. PERFORMANCE LOGGING
-- ==========================================================
CREATE TABLE query_metrics (
    id SERIAL PRIMARY KEY,
    query_desc VARCHAR(100),
    execution_time_ms NUMERIC(10, 3),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);