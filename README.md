# Stock Market ETL & Analytics Pipeline

End-to-end data pipeline that extracts daily US stock market data, loads into a PostgreSQL data warehouse (star schema), calculates technical indicators, and sends automated email reports with charts and alerts.

## Architecture

```
                            ┌─────────────────────────────────────────────┐
                            │            Apache Airflow                    │
                            │                                             │
  Yahoo Finance API ──────► │  DAG 1: ETL                                 │
                            │  Extract → Transform → Load                 │
                            │       │                                     │
                            │       ▼                                     │
                            │  DAG 2: Analytics                           │
                            │  Moving Avg ┐                               │
                            │  Volatility ├→ Alerts → Charts → Email     │
                            │  Returns   ┘                               │
                            └──────────────────┬──────────────────────────┘
                                               │
                                               ▼
                                    ┌─────────────────────┐
                                    │  PostgreSQL 15       │
                                    │  Star Schema + DW    │
                                    └─────────────────────┘
```

### Airflow DAG — Analytics Pipeline

<img src="docs/graph.png" alt="Airflow DAG Graph View" width="800"/>

## Tech Stack

| Component        | Technology                          |
|------------------|-------------------------------------|
| Orchestration    | Apache Airflow 2.8                  |
| Extraction       | yfinance (Yahoo Finance API)        |
| Transformation   | pandas                              |
| Data Warehouse   | PostgreSQL 15 (Star Schema)         |
| Analytics        | pandas, NumPy                       |
| Visualization    | matplotlib                          |
| Email Reports    | SMTP (Gmail) with embedded charts   |
| Infrastructure   | Docker / Docker Compose             |

## Data Model (Star Schema)

```
┌──────────────┐       ┌───────────────────┐       ┌──────────────┐
│  dim_company │──────►│ fact_stock_price   │◄──────│   dim_date   │
│              │       │                   │       │              │
│ ticker       │       │ open/high/low/    │       │ full_date    │
│ name         │       │ close/volume      │       │ year/month   │
│ sector       │       │ daily_return      │       │ day_of_week  │
│ industry     │       │                   │       │ is_weekend   │
└──────────────┘       └───────────────────┘       └──────────────┘
                       ┌───────────────────┐
                       │ fact_daily_summary │
                       │                   │
                       │ avg_return         │
                       │ best/worst ticker  │
                       │ total_volume       │
                       └───────────────────┘

┌────────────────────────┐  ┌────────────────────────┐  ┌────────────────────────┐
│ analytics_moving_avg   │  │ analytics_volatility   │  │ analytics_returns      │
│                        │  │                        │  │                        │
│ SMA-9 / SMA-21 / EMA-9│  │ volatility 30d / 60d   │  │ return 7d / 30d / 90d  │
│ BUY / SELL / HOLD      │  │ volume ratio           │  │                        │
└────────────────────────┘  └────────────────────────┘  └────────────────────────┘

┌────────────────────────┐
│ analytics_alerts       │
│                        │
│ BIG_DROP / BIG_GAIN    │
│ HIGH_VOLUME / SIGNALS  │
│ severity levels        │
└────────────────────────┘
```

### Data Warehouse Tables

<img src="docs/bd1.png" alt="Star Schema Tables" width="500"/>

<img src="docs/bd2.png" alt="dim_company data" width="800"/>

## Features

- **Automated ETL**: Daily extraction of stock prices from Yahoo Finance
- **Star Schema DW**: Proper dimensional modeling with fact and dimension tables
- **Technical Indicators**: SMA-9, SMA-21, EMA-9 with BUY/SELL/HOLD signals
- **Volatility Analysis**: 30-day and 60-day volatility, volume anomaly detection
- **Accumulated Returns**: 7-day, 30-day, and 90-day performance tracking
- **Smart Alerts**: Automatic detection of big drops (>3%), big gains, abnormal volume
- **Daily Email Reports**: HTML emails with embedded charts sent via Gmail
- **Fully Containerized**: One command to start the entire pipeline

## Output Examples

### Daily Email Report

The pipeline sends automated daily reports via email with performance analysis, returns, volume, and trading signals.

**Performance Overview & Alerts:**

<img src="docs/email1.png" alt="Email Report - Performance" width="600"/>

**Daily Returns (gains in green, losses in red):**

<img src="docs/email2.png" alt="Email Report - Daily Returns" width="600"/>

**Trading Volume & SMA Signals:**

<img src="docs/email3.png" alt="Email Report - Volume and Signals" width="600"/>

### Alert Types

| Alert          | Trigger                      | Severity |
|----------------|------------------------------|----------|
| BIG_DROP       | Daily return < -3%           | WARNING / CRITICAL (< -5%) |
| BIG_GAIN       | Daily return > +3%           | INFO     |
| SIGNAL_BUY     | SMA-9 crosses above SMA-21   | INFO     |
| SIGNAL_SELL    | SMA-9 crosses below SMA-21   | INFO     |
| HIGH_VOLUME    | Volume > 1.5x 30-day average | WARNING  |

## Stocks Tracked

| Ticker | Company           | Sector                 |
|--------|-------------------|------------------------|
| AAPL   | Apple             | Technology             |
| MSFT   | Microsoft         | Technology             |
| GOOGL  | Alphabet          | Communication Services |
| AMZN   | Amazon            | Consumer Cyclical      |
| NVDA   | NVIDIA            | Technology             |
| META   | Meta Platforms    | Communication Services |
| TSLA   | Tesla             | Consumer Cyclical      |
| JPM    | JPMorgan Chase    | Financial Services     |
| V      | Visa              | Financial Services     |
| WMT    | Walmart           | Consumer Defensive     |

## Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Gmail account with App Password (for email reports)

### 1. Clone the repository

```bash
git clone https://github.com/willrosa93/stock-market-etl.git
cd stock-market-etl
```

### 2. Configure email (optional)

Create a `.env` file:

```bash
cp .env.example .env
```

Edit `.env` and add your Gmail App Password:

```
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
```

> To generate a Gmail App Password: [Google Account > Security > 2FA > App Passwords](https://myaccount.google.com/apppasswords)

### 3. Start the pipeline

```bash
docker-compose up -d
```

### 4. Initialize Airflow

```bash
docker-compose run --rm airflow-init
docker-compose restart airflow-webserver airflow-scheduler
```

### 5. Access Airflow UI

- URL: http://localhost:8081
- Username: `admin`
- Password: `admin`

### 6. Run the pipeline

1. Enable the `stock_market_etl` DAG and trigger it manually
2. Enable the `stock_market_analytics` DAG and trigger it after ETL completes
3. Both DAGs run automatically on weekdays (ETL at 8pm, Analytics at 8:30pm)

### 7. Query the data warehouse

```bash
psql -h localhost -p 5434 -U warehouse -d stock_warehouse
```

```sql
-- Latest prices with signals
SELECT
    c.ticker, c.name, d.full_date,
    p.close_price, p.daily_return,
    ma.sma_9, ma.sma_21, ma.signal
FROM fact_stock_price p
JOIN dim_company c ON c.company_id = p.company_id
JOIN dim_date d ON d.date_id = p.date_id
LEFT JOIN analytics_moving_averages ma ON ma.company_id = p.company_id AND ma.date_id = p.date_id
ORDER BY d.full_date DESC, c.ticker
LIMIT 20;

-- Performance ranking
SELECT
    c.ticker, c.name,
    r.return_7d * 100 AS "7d %",
    r.return_30d * 100 AS "30d %",
    v.volatility_30d
FROM analytics_returns r
JOIN dim_company c ON c.company_id = r.company_id
JOIN dim_date d ON d.date_id = r.date_id
LEFT JOIN analytics_volatility v ON v.company_id = r.company_id AND v.date_id = r.date_id
WHERE d.full_date = (SELECT MAX(full_date) FROM dim_date dd JOIN analytics_returns ar ON ar.date_id = dd.date_id)
ORDER BY r.return_7d DESC;
```

## Project Structure

```
.
├── dags/
│   ├── stock_etl_dag.py            # ETL pipeline DAG
│   ├── stock_analytics_dag.py      # Analytics & reporting DAG
│   └── scripts/
│       ├── extract.py              # Yahoo Finance data extraction
│       ├── transform.py            # Data cleaning & transformation
│       ├── load.py                 # PostgreSQL loading (upserts)
│       ├── analytics.py            # Technical indicators & alerts
│       └── report.py               # Chart generation & email sending
├── sql/
│   ├── create_tables.sql           # Star schema DDL
│   └── create_analytics_tables.sql # Analytics tables DDL
├── docs/                           # Screenshots and documentation
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

## DAG Pipelines

### DAG 1: `stock_market_etl` (weekdays at 20:00)

```
extract_stock_data → transform_stock_data → load_dim_company → load_fact_stock_price → load_fact_daily_summary
```

### DAG 2: `stock_market_analytics` (weekdays at 20:30)

```
┌─ calculate_moving_averages ─┐
├─ calculate_volatility ──────┼─► generate_alerts → generate_charts → send_email_report
└─ calculate_returns ─────────┘
```
