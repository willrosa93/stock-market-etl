-- Star Schema for Stock Market Data

-- Dimension: Companies/Tickers
CREATE TABLE IF NOT EXISTS dim_company (
    company_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(200),
    sector VARCHAR(100),
    industry VARCHAR(100),
    country VARCHAR(50),
    currency VARCHAR(10),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY, -- format YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    week_of_year INTEGER NOT NULL
);

-- Fact: Daily Stock Prices
CREATE TABLE IF NOT EXISTS fact_stock_price (
    price_id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES dim_company(company_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    open_price NUMERIC(12,4),
    high_price NUMERIC(12,4),
    low_price NUMERIC(12,4),
    close_price NUMERIC(12,4),
    adj_close_price NUMERIC(12,4),
    volume BIGINT,
    daily_return NUMERIC(10,6),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(company_id, date_id)
);

-- Fact: Daily Aggregated Metrics
CREATE TABLE IF NOT EXISTS fact_daily_summary (
    summary_id SERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    total_tickers INTEGER,
    avg_daily_return NUMERIC(10,6),
    best_ticker VARCHAR(10),
    best_return NUMERIC(10,6),
    worst_ticker VARCHAR(10),
    worst_return NUMERIC(10,6),
    total_volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(date_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_date ON fact_stock_price(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_company ON fact_stock_price(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_company_date ON fact_stock_price(company_id, date_id);
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);

-- Populate dim_date for 2020-2030
INSERT INTO dim_date (date_id, full_date, year, month, day, day_of_week, day_name, month_name, quarter, is_weekend, week_of_year)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_id,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    EXTRACT(DAY FROM d)::INTEGER AS day,
    EXTRACT(DOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(DOW FROM d) IN (0, 6) AS is_weekend,
    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_id) DO NOTHING;
