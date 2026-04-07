"""Extract stock data from Yahoo Finance using yfinance."""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf

logger = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")


def extract_stock_data(tickers: list[str], **context) -> str:
    """Download daily stock data for the given tickers.

    Extracts the last 5 business days of data to handle weekends
    and holidays gracefully.
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    logger.info("Extracting data for %d tickers: %s", len(tickers), tickers)
    logger.info("Period: %s to %s", start_date.date(), end_date.date())

    all_data = {}
    company_info = {}

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date)

            if hist.empty:
                logger.warning("No data returned for %s", ticker)
                continue

            records = []
            for date, row in hist.iterrows():
                records.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "open": round(float(row["Open"]), 4),
                    "high": round(float(row["High"]), 4),
                    "low": round(float(row["Low"]), 4),
                    "close": round(float(row["Close"]), 4),
                    "volume": int(row["Volume"]),
                })

            all_data[ticker] = records

            info = stock.info
            company_info[ticker] = {
                "name": info.get("longName", ticker),
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "country": info.get("country", "Brazil"),
                "currency": info.get("currency", "BRL"),
            }

            logger.info("Extracted %d records for %s", len(records), ticker)

        except Exception as e:
            logger.error("Failed to extract %s: %s", ticker, e)

    output = {
        "extraction_date": datetime.now().isoformat(),
        "tickers_requested": len(tickers),
        "tickers_extracted": len(all_data),
        "prices": all_data,
        "companies": company_info,
    }

    output_path = DATA_DIR / "raw_stock_data.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    logger.info("Raw data saved to %s", output_path)
    return str(output_path)
