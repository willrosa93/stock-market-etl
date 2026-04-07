"""Load transformed data into PostgreSQL data warehouse."""

import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")


def _get_engine():
    """Create SQLAlchemy engine for the warehouse database."""
    host = os.environ.get("WAREHOUSE_DB_HOST", "warehouse-db")
    port = os.environ.get("WAREHOUSE_DB_PORT", "5432")
    user = os.environ.get("WAREHOUSE_DB_USER", "warehouse")
    password = os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse")
    db = os.environ.get("WAREHOUSE_DB_NAME", "stock_warehouse")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def load_dim_company(**context):
    """Upsert company dimension data."""
    df = pd.read_csv(DATA_DIR / "transformed_companies.csv")
    engine = _get_engine()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_company (ticker, name, sector, industry, country, currency)
                    VALUES (:ticker, :name, :sector, :industry, :country, :currency)
                    ON CONFLICT (ticker) DO UPDATE SET
                        name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        industry = EXCLUDED.industry,
                        country = EXCLUDED.country,
                        currency = EXCLUDED.currency,
                        updated_at = NOW()
                """),
                {
                    "ticker": row["ticker"],
                    "name": row["name"],
                    "sector": row["sector"],
                    "industry": row["industry"],
                    "country": row["country"],
                    "currency": row["currency"],
                },
            )

    logger.info("Loaded %d companies into dim_company", len(df))


def load_fact_stock_price(**context):
    """Upsert stock price facts."""
    df = pd.read_csv(DATA_DIR / "transformed_prices.csv")
    engine = _get_engine()

    with engine.begin() as conn:
        # Get company_id mapping
        result = conn.execute(text("SELECT company_id, ticker FROM dim_company"))
        company_map = {row[1]: row[0] for row in result}

        loaded = 0
        for _, row in df.iterrows():
            company_id = company_map.get(row["ticker"])
            if not company_id:
                logger.warning("Ticker %s not found in dim_company, skipping", row["ticker"])
                continue

            conn.execute(
                text("""
                    INSERT INTO fact_stock_price
                        (company_id, date_id, open_price, high_price, low_price,
                         close_price, adj_close_price, volume, daily_return)
                    VALUES
                        (:company_id, :date_id, :open_price, :high_price, :low_price,
                         :close_price, :adj_close_price, :volume, :daily_return)
                    ON CONFLICT (company_id, date_id) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        adj_close_price = EXCLUDED.adj_close_price,
                        volume = EXCLUDED.volume,
                        daily_return = EXCLUDED.daily_return
                """),
                {
                    "company_id": company_id,
                    "date_id": int(row["date_id"]),
                    "open_price": row["open_price"],
                    "high_price": row["high_price"],
                    "low_price": row["low_price"],
                    "close_price": row["close_price"],
                    "adj_close_price": row["adj_close_price"],
                    "volume": int(row["volume"]),
                    "daily_return": row["daily_return"] if pd.notna(row["daily_return"]) else None,
                },
            )
            loaded += 1

    logger.info("Loaded %d price records into fact_stock_price", loaded)


def load_fact_daily_summary(**context):
    """Upsert daily summary facts."""
    df = pd.read_csv(DATA_DIR / "transformed_summary.csv")
    engine = _get_engine()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO fact_daily_summary
                        (date_id, total_tickers, avg_daily_return, best_ticker,
                         best_return, worst_ticker, worst_return, total_volume)
                    VALUES
                        (:date_id, :total_tickers, :avg_daily_return, :best_ticker,
                         :best_return, :worst_ticker, :worst_return, :total_volume)
                    ON CONFLICT (date_id) DO UPDATE SET
                        total_tickers = EXCLUDED.total_tickers,
                        avg_daily_return = EXCLUDED.avg_daily_return,
                        best_ticker = EXCLUDED.best_ticker,
                        best_return = EXCLUDED.best_return,
                        worst_ticker = EXCLUDED.worst_ticker,
                        worst_return = EXCLUDED.worst_return,
                        total_volume = EXCLUDED.total_volume
                """),
                {
                    "date_id": int(row["date_id"]),
                    "total_tickers": int(row["total_tickers"]),
                    "avg_daily_return": row["avg_daily_return"] if pd.notna(row["avg_daily_return"]) else None,
                    "best_ticker": row["best_ticker"],
                    "best_return": row["best_return"] if pd.notna(row["best_return"]) else None,
                    "worst_ticker": row["worst_ticker"],
                    "worst_return": row["worst_return"] if pd.notna(row["worst_return"]) else None,
                    "total_volume": int(row["total_volume"]),
                },
            )

    logger.info("Loaded %d daily summaries into fact_daily_summary", len(df))
