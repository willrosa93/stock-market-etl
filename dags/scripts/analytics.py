"""Calculate technical indicators and analytics from stock data."""

import logging
import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def _get_engine():
    host = os.environ.get("WAREHOUSE_DB_HOST", "warehouse-db")
    port = os.environ.get("WAREHOUSE_DB_PORT", "5432")
    user = os.environ.get("WAREHOUSE_DB_USER", "warehouse")
    password = os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse")
    db = os.environ.get("WAREHOUSE_DB_NAME", "stock_warehouse")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def calculate_moving_averages(**context):
    """Calculate SMA-9, SMA-21 and generate BUY/SELL/HOLD signals."""
    engine = _get_engine()

    df = pd.read_sql("""
        SELECT p.company_id, p.date_id, d.full_date, p.close_price, c.ticker
        FROM fact_stock_price p
        JOIN dim_date d ON d.date_id = p.date_id
        JOIN dim_company c ON c.company_id = p.company_id
        ORDER BY p.company_id, d.full_date
    """, engine)

    if df.empty:
        logger.warning("No price data found")
        return

    results = []
    for ticker, group in df.groupby("company_id"):
        group = group.sort_values("full_date")
        group["sma_9"] = group["close_price"].rolling(window=9, min_periods=1).mean()
        group["sma_21"] = group["close_price"].rolling(window=21, min_periods=1).mean()

        # EMA-9
        group["ema_9"] = group["close_price"].ewm(span=9, min_periods=1, adjust=False).mean()

        # Signal: SMA-9 crosses above SMA-21 = BUY, below = SELL
        group["signal"] = "HOLD"
        group.loc[group["sma_9"] > group["sma_21"], "signal"] = "BUY"
        group.loc[group["sma_9"] < group["sma_21"], "signal"] = "SELL"

        results.append(group)

    df_result = pd.concat(results)

    with engine.begin() as conn:
        for _, row in df_result.iterrows():
            conn.execute(
                text("""
                    INSERT INTO analytics_moving_averages
                        (company_id, date_id, close_price, sma_9, sma_21, ema_9, signal)
                    VALUES (:company_id, :date_id, :close_price, :sma_9, :sma_21, :ema_9, :signal)
                    ON CONFLICT (company_id, date_id) DO UPDATE SET
                        close_price = EXCLUDED.close_price,
                        sma_9 = EXCLUDED.sma_9,
                        sma_21 = EXCLUDED.sma_21,
                        ema_9 = EXCLUDED.ema_9,
                        signal = EXCLUDED.signal
                """),
                {
                    "company_id": int(row["company_id"]),
                    "date_id": int(row["date_id"]),
                    "close_price": round(float(row["close_price"]), 4),
                    "sma_9": round(float(row["sma_9"]), 4),
                    "sma_21": round(float(row["sma_21"]), 4),
                    "ema_9": round(float(row["ema_9"]), 4),
                    "signal": row["signal"],
                },
            )

    logger.info("Calculated moving averages for %d records", len(df_result))


def calculate_volatility(**context):
    """Calculate 30d and 60d volatility and volume ratios."""
    engine = _get_engine()

    df = pd.read_sql("""
        SELECT p.company_id, p.date_id, d.full_date, p.daily_return, p.volume, c.ticker
        FROM fact_stock_price p
        JOIN dim_date d ON d.date_id = p.date_id
        JOIN dim_company c ON c.company_id = p.company_id
        ORDER BY p.company_id, d.full_date
    """, engine)

    if df.empty:
        return

    results = []
    for company_id, group in df.groupby("company_id"):
        group = group.sort_values("full_date")
        group["volatility_30d"] = group["daily_return"].rolling(window=30, min_periods=2).std()
        group["volatility_60d"] = group["daily_return"].rolling(window=60, min_periods=2).std()
        group["avg_volume_30d"] = group["volume"].rolling(window=30, min_periods=1).mean()
        group["volume_ratio"] = group["volume"] / group["avg_volume_30d"]
        results.append(group)

    df_result = pd.concat(results)

    with engine.begin() as conn:
        for _, row in df_result.iterrows():
            conn.execute(
                text("""
                    INSERT INTO analytics_volatility
                        (company_id, date_id, volatility_30d, volatility_60d, avg_volume_30d, volume_ratio)
                    VALUES (:company_id, :date_id, :vol30, :vol60, :avg_vol, :vol_ratio)
                    ON CONFLICT (company_id, date_id) DO UPDATE SET
                        volatility_30d = EXCLUDED.volatility_30d,
                        volatility_60d = EXCLUDED.volatility_60d,
                        avg_volume_30d = EXCLUDED.avg_volume_30d,
                        volume_ratio = EXCLUDED.volume_ratio
                """),
                {
                    "company_id": int(row["company_id"]),
                    "date_id": int(row["date_id"]),
                    "vol30": round(float(row["volatility_30d"]), 6) if pd.notna(row["volatility_30d"]) else None,
                    "vol60": round(float(row["volatility_60d"]), 6) if pd.notna(row["volatility_60d"]) else None,
                    "avg_vol": round(float(row["avg_volume_30d"]), 2) if pd.notna(row["avg_volume_30d"]) else None,
                    "vol_ratio": round(float(row["volume_ratio"]), 4) if pd.notna(row["volume_ratio"]) else None,
                },
            )

    logger.info("Calculated volatility for %d records", len(df_result))


def calculate_returns(**context):
    """Calculate accumulated returns over 7d, 30d, 90d windows."""
    engine = _get_engine()

    df = pd.read_sql("""
        SELECT p.company_id, p.date_id, d.full_date, p.close_price
        FROM fact_stock_price p
        JOIN dim_date d ON d.date_id = p.date_id
        ORDER BY p.company_id, d.full_date
    """, engine)

    if df.empty:
        return

    results = []
    for company_id, group in df.groupby("company_id"):
        group = group.sort_values("full_date")
        for window, col in [(7, "return_7d"), (30, "return_30d"), (90, "return_90d")]:
            group[col] = group["close_price"].pct_change(periods=min(window, len(group) - 1))
        results.append(group)

    df_result = pd.concat(results)

    with engine.begin() as conn:
        for _, row in df_result.iterrows():
            conn.execute(
                text("""
                    INSERT INTO analytics_returns
                        (company_id, date_id, return_7d, return_30d, return_90d)
                    VALUES (:company_id, :date_id, :r7, :r30, :r90)
                    ON CONFLICT (company_id, date_id) DO UPDATE SET
                        return_7d = EXCLUDED.return_7d,
                        return_30d = EXCLUDED.return_30d,
                        return_90d = EXCLUDED.return_90d
                """),
                {
                    "company_id": int(row["company_id"]),
                    "date_id": int(row["date_id"]),
                    "r7": round(float(row["return_7d"]), 6) if pd.notna(row["return_7d"]) else None,
                    "r30": round(float(row["return_30d"]), 6) if pd.notna(row["return_30d"]) else None,
                    "r90": round(float(row["return_90d"]), 6) if pd.notna(row["return_90d"]) else None,
                },
            )

    logger.info("Calculated returns for %d records", len(df_result))


def generate_alerts(**context):
    """Generate alerts for significant market events."""
    engine = _get_engine()

    # Get latest date
    latest = pd.read_sql("""
        SELECT MAX(d.full_date) as latest_date
        FROM fact_stock_price p
        JOIN dim_date d ON d.date_id = p.date_id
    """, engine)

    if latest.empty or latest["latest_date"].iloc[0] is None:
        return []

    latest_date = latest["latest_date"].iloc[0]

    # Get latest day data with all indicators
    df = pd.read_sql(text("""
        SELECT
            c.ticker, c.name, d.full_date, d.date_id,
            p.close_price, p.daily_return, p.volume, p.company_id,
            ma.sma_9, ma.sma_21, ma.signal,
            v.volatility_30d, v.volume_ratio,
            r.return_7d, r.return_30d
        FROM fact_stock_price p
        JOIN dim_company c ON c.company_id = p.company_id
        JOIN dim_date d ON d.date_id = p.date_id
        LEFT JOIN analytics_moving_averages ma ON ma.company_id = p.company_id AND ma.date_id = p.date_id
        LEFT JOIN analytics_volatility v ON v.company_id = p.company_id AND v.date_id = p.date_id
        LEFT JOIN analytics_returns r ON r.company_id = p.company_id AND r.date_id = p.date_id
        WHERE d.full_date = :latest_date
        ORDER BY c.ticker
    """), engine, params={"latest_date": latest_date})

    alerts = []

    for _, row in df.iterrows():
        # Alert: big daily drop (> 3%)
        if pd.notna(row["daily_return"]) and row["daily_return"] < -0.03:
            alerts.append({
                "company_id": int(row["company_id"]),
                "date_id": int(row["date_id"]),
                "alert_type": "BIG_DROP",
                "message": f"{row['ticker']} ({row['name']}) caiu {row['daily_return']*100:.2f}% hoje. Preço: ${row['close_price']:.2f}",
                "severity": "CRITICAL" if row["daily_return"] < -0.05 else "WARNING",
            })

        # Alert: big daily gain (> 3%)
        if pd.notna(row["daily_return"]) and row["daily_return"] > 0.03:
            alerts.append({
                "company_id": int(row["company_id"]),
                "date_id": int(row["date_id"]),
                "alert_type": "BIG_GAIN",
                "message": f"{row['ticker']} ({row['name']}) subiu +{row['daily_return']*100:.2f}% hoje. Preço: ${row['close_price']:.2f}",
                "severity": "INFO",
            })

        # Alert: BUY/SELL signal change
        if row["signal"] in ("BUY", "SELL"):
            alerts.append({
                "company_id": int(row["company_id"]),
                "date_id": int(row["date_id"]),
                "alert_type": f"SIGNAL_{row['signal']}",
                "message": f"{row['ticker']}: Sinal de {row['signal']} (SMA-9 {'>' if row['signal']=='BUY' else '<'} SMA-21). Preço: ${row['close_price']:.2f}",
                "severity": "INFO",
            })

        # Alert: abnormal volume (> 1.5x average)
        if pd.notna(row["volume_ratio"]) and row["volume_ratio"] > 1.5:
            alerts.append({
                "company_id": int(row["company_id"]),
                "date_id": int(row["date_id"]),
                "alert_type": "HIGH_VOLUME",
                "message": f"{row['ticker']}: Volume {row['volume_ratio']:.1f}x acima da média de 30 dias ({int(row['volume']):,} ações)",
                "severity": "WARNING",
            })

    # Save alerts to DB
    with engine.begin() as conn:
        for alert in alerts:
            conn.execute(
                text("""
                    INSERT INTO analytics_alerts (company_id, date_id, alert_type, message, severity)
                    VALUES (:company_id, :date_id, :alert_type, :message, :severity)
                """),
                alert,
            )

    logger.info("Generated %d alerts for %s", len(alerts), latest_date)

    # Push alerts to XCom for email task
    context["ti"].xcom_push(key="alerts", value=alerts)
    context["ti"].xcom_push(key="report_date", value=str(latest_date))
    return alerts
