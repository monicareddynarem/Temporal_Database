
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

CREATE TABLE raw_ticks (
    ts TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
    price NUMERIC(10, 2) NOT NULL,
    volume INTEGER NOT NULL
) PARTITION BY RANGE (ts);

CREATE TABLE ohlcv_1s (
    ts_bucket TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    volume INTEGER,
    -- PKs on partitioned tables must include the partition key (ts_bucket)
    PRIMARY KEY (ts_bucket, symbol)
) PARTITION BY RANGE (ts_bucket);


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

CREATE TABLE aggregation_watermarks (
    aggregation_interval VARCHAR(10) PRIMARY KEY, 
    last_processed_ts TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE query_metrics (
    id SERIAL PRIMARY KEY,
    query_desc VARCHAR(100),
    execution_time_ms NUMERIC(10, 3),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);