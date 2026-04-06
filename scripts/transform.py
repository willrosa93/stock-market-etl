"""Transform raw stock data into warehouse-ready format."""

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")


def transform_stock_data(**context) -> str:
    """Transform raw JSON data into clean DataFrames saved as CSV."""
    raw_path = DATA_DIR / "raw_stock_data.json"

    with open(raw_path) as f:
        raw = json.load(f)

    # --- Transform company dimension ---
    companies = []
    for ticker, info in raw["companies"].items():
        companies.append({
            "ticker": ticker,
            "name": info["name"],
            "sector": info["sector"],
            "industry": info["industry"],
            "country": info["country"],
            "currency": info["currency"],
        })

    df_companies = pd.DataFrame(companies)
    companies_path = DATA_DIR / "transformed_companies.csv"
    df_companies.to_csv(companies_path, index=False)
    logger.info("Transformed %d companies", len(df_companies))

    # --- Transform stock prices ---
    all_prices = []
    for ticker, records in raw["prices"].items():
        for record in records:
            all_prices.append({
                "ticker": ticker,
                "date": record["date"],
                "open_price": record["open"],
                "high_price": record["high"],
                "low_price": record["low"],
                "close_price": record["close"],
                "adj_close_price": record["close"],  # yfinance already adjusts
                "volume": record["volume"],
            })

    df_prices = pd.DataFrame(all_prices)

    if df_prices.empty:
        logger.warning("No price data to transform")
        df_prices.to_csv(DATA_DIR / "transformed_prices.csv", index=False)
        pd.DataFrame().to_csv(DATA_DIR / "transformed_summary.csv", index=False)
        return str(DATA_DIR)

    # Calculate daily return per ticker
    df_prices = df_prices.sort_values(["ticker", "date"])
    df_prices["daily_return"] = (
        df_prices.groupby("ticker")["close_price"]
        .pct_change()
        .round(6)
    )

    # date_id in YYYYMMDD format
    df_prices["date_id"] = pd.to_datetime(df_prices["date"]).dt.strftime("%Y%m%d").astype(int)

    prices_path = DATA_DIR / "transformed_prices.csv"
    df_prices.to_csv(prices_path, index=False)
    logger.info("Transformed %d price records", len(df_prices))

    # --- Build daily summary ---
    daily = df_prices.groupby("date").agg(
        total_tickers=("ticker", "nunique"),
        avg_daily_return=("daily_return", "mean"),
        total_volume=("volume", "sum"),
    ).reset_index()

    # Best and worst ticker per day
    best = df_prices.loc[df_prices.groupby("date")["daily_return"].idxmax()][["date", "ticker", "daily_return"]]
    best.columns = ["date", "best_ticker", "best_return"]

    worst = df_prices.loc[df_prices.groupby("date")["daily_return"].idxmin()][["date", "ticker", "daily_return"]]
    worst.columns = ["date", "worst_ticker", "worst_return"]

    daily = daily.merge(best, on="date").merge(worst, on="date")
    daily["date_id"] = pd.to_datetime(daily["date"]).dt.strftime("%Y%m%d").astype(int)

    summary_path = DATA_DIR / "transformed_summary.csv"
    daily.to_csv(summary_path, index=False)
    logger.info("Transformed %d daily summaries", len(daily))

    return str(DATA_DIR)
