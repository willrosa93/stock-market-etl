-- Analytics tables for stock market analysis

-- Moving averages and technical indicators
CREATE TABLE IF NOT EXISTS analytics_moving_averages (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES dim_company(company_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    close_price NUMERIC(12,4),
    sma_9 NUMERIC(12,4),
    sma_21 NUMERIC(12,4),
    ema_9 NUMERIC(12,4),
    signal VARCHAR(10), -- 'BUY', 'SELL', 'HOLD'
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(company_id, date_id)
);

-- Volatility metrics
CREATE TABLE IF NOT EXISTS analytics_volatility (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES dim_company(company_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    volatility_30d NUMERIC(10,6),
    volatility_60d NUMERIC(10,6),
    avg_volume_30d NUMERIC(16,2),
    volume_ratio NUMERIC(10,4), -- current volume / avg 30d
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(company_id, date_id)
);

-- Accumulated returns
CREATE TABLE IF NOT EXISTS analytics_returns (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES dim_company(company_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    return_7d NUMERIC(10,6),
    return_30d NUMERIC(10,6),
    return_90d NUMERIC(10,6),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(company_id, date_id)
);

-- Alert history
CREATE TABLE IF NOT EXISTS analytics_alerts (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES dim_company(company_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    alert_type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    severity VARCHAR(10) NOT NULL, -- 'INFO', 'WARNING', 'CRITICAL'
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analytics_ma_company_date ON analytics_moving_averages(company_id, date_id);
CREATE INDEX IF NOT EXISTS idx_analytics_vol_company_date ON analytics_volatility(company_id, date_id);
CREATE INDEX IF NOT EXISTS idx_analytics_ret_company_date ON analytics_returns(company_id, date_id);
CREATE INDEX IF NOT EXISTS idx_analytics_alerts_date ON analytics_alerts(date_id);
