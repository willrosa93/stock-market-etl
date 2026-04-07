"""Generate charts and send daily email report."""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from io import BytesIO
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

CHARTS_DIR = Path("/opt/airflow/data/charts")


def _get_engine():
    host = os.environ.get("WAREHOUSE_DB_HOST", "warehouse-db")
    port = os.environ.get("WAREHOUSE_DB_PORT", "5432")
    user = os.environ.get("WAREHOUSE_DB_USER", "warehouse")
    password = os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse")
    db = os.environ.get("WAREHOUSE_DB_NAME", "stock_warehouse")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def generate_charts(**context):
    """Generate PNG charts for the email report."""
    engine = _get_engine()
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Chart 1: Price evolution of all tickers ---
    df = pd.read_sql("""
        SELECT c.ticker, d.full_date, p.close_price
        FROM fact_stock_price p
        JOIN dim_company c ON c.company_id = p.company_id
        JOIN dim_date d ON d.date_id = p.date_id
        ORDER BY d.full_date
    """, engine)

    if df.empty:
        logger.warning("No data for charts")
        return

    df["full_date"] = pd.to_datetime(df["full_date"])

    # Normalize prices to percentage change from first day
    fig, ax = plt.subplots(figsize=(12, 6))
    for ticker, group in df.groupby("ticker"):
        group = group.sort_values("full_date")
        base = group["close_price"].iloc[0]
        pct = ((group["close_price"] / base) - 1) * 100
        ax.plot(group["full_date"], pct, label=ticker, linewidth=2)

    ax.set_title("Stock Performance (% change from first day)", fontsize=14, fontweight="bold")
    ax.set_ylabel("Change (%)")
    ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "performance.png", dpi=150, bbox_inches="tight")
    plt.close()

    # --- Chart 2: Daily returns heatmap-style bar chart ---
    latest_date = df["full_date"].max()
    df_latest = df[df["full_date"] == latest_date].copy()

    if not df_latest.empty:
        returns = pd.read_sql(text("""
            SELECT c.ticker, p.daily_return
            FROM fact_stock_price p
            JOIN dim_company c ON c.company_id = p.company_id
            JOIN dim_date d ON d.date_id = p.date_id
            WHERE d.full_date = :dt
            ORDER BY p.daily_return DESC
        """), engine, params={"dt": latest_date})

        if not returns.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            colors = ["#2ecc71" if r >= 0 else "#e74c3c" for r in returns["daily_return"]]
            bars = ax.barh(returns["ticker"], returns["daily_return"] * 100, color=colors)

            for bar, val in zip(bars, returns["daily_return"]):
                ax.text(
                    bar.get_width() + (0.05 if val >= 0 else -0.05),
                    bar.get_y() + bar.get_height() / 2,
                    f"{val*100:+.2f}%",
                    va="center", fontsize=10,
                )

            ax.set_title(f"Daily Returns - {latest_date.strftime('%d/%m/%Y')}", fontsize=14, fontweight="bold")
            ax.set_xlabel("Return (%)")
            ax.axvline(x=0, color="black", linewidth=0.5)
            ax.grid(True, axis="x", alpha=0.3)
            plt.tight_layout()
            plt.savefig(CHARTS_DIR / "daily_returns.png", dpi=150, bbox_inches="tight")
            plt.close()

    # --- Chart 3: Volume comparison ---
    df_vol = pd.read_sql(text("""
        SELECT c.ticker, p.volume
        FROM fact_stock_price p
        JOIN dim_company c ON c.company_id = p.company_id
        JOIN dim_date d ON d.date_id = p.date_id
        WHERE d.full_date = :dt
        ORDER BY p.volume DESC
    """), engine, params={"dt": latest_date})

    if not df_vol.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(df_vol["ticker"], df_vol["volume"] / 1e6, color="#3498db")
        ax.set_title(f"Trading Volume (millions) - {latest_date.strftime('%d/%m/%Y')}", fontsize=14, fontweight="bold")
        ax.set_xlabel("Volume (M)")
        ax.grid(True, axis="x", alpha=0.3)
        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "volume.png", dpi=150, bbox_inches="tight")
        plt.close()

    # --- Chart 4: Signals overview ---
    signals = pd.read_sql(text("""
        SELECT c.ticker, ma.signal, ma.sma_9, ma.sma_21, ma.close_price
        FROM analytics_moving_averages ma
        JOIN dim_company c ON c.company_id = ma.company_id
        JOIN dim_date d ON d.date_id = ma.date_id
        WHERE d.full_date = :dt
        ORDER BY c.ticker
    """), engine, params={"dt": latest_date})

    if not signals.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        color_map = {"BUY": "#2ecc71", "SELL": "#e74c3c", "HOLD": "#f39c12"}
        colors = [color_map.get(s, "#95a5a6") for s in signals["signal"]]
        ax.barh(signals["ticker"], [1] * len(signals), color=colors)

        for i, (_, row) in enumerate(signals.iterrows()):
            ax.text(0.5, i, f"{row['signal']} | ${row['close_price']:.2f} | SMA9: ${row['sma_9']:.2f} | SMA21: ${row['sma_21']:.2f}",
                    ha="center", va="center", fontsize=10, fontweight="bold", color="white")

        ax.set_title(f"Trading Signals - {latest_date.strftime('%d/%m/%Y')}", fontsize=14, fontweight="bold")
        ax.set_xlim(0, 1)
        ax.set_xticks([])
        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "signals.png", dpi=150, bbox_inches="tight")
        plt.close()

    logger.info("Charts saved to %s", CHARTS_DIR)


def send_email_report(**context):
    """Send daily report email with charts and alerts."""
    ti = context["ti"]
    alerts = ti.xcom_pull(task_ids="generate_alerts", key="alerts") or []
    report_date = ti.xcom_pull(task_ids="generate_alerts", key="report_date") or "N/A"

    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_password = os.environ.get("SMTP_PASSWORD", "")
    email_to = os.environ.get("EMAIL_TO", "willianrosa993@gmail.com")

    if not smtp_user or not smtp_password:
        logger.warning("SMTP credentials not configured. Skipping email.")
        logger.info("Set SMTP_USER and SMTP_PASSWORD environment variables.")
        logger.info("Report date: %s | Alerts: %d", report_date, len(alerts))
        for a in alerts:
            logger.info("[%s] %s - %s", a["severity"], a["alert_type"], a["message"])
        return

    # Build HTML email
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; }}
            h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
            h2 {{ color: #34495e; }}
            .alert {{ padding: 10px; margin: 5px 0; border-radius: 5px; }}
            .CRITICAL {{ background-color: #e74c3c; color: white; }}
            .WARNING {{ background-color: #f39c12; color: white; }}
            .INFO {{ background-color: #3498db; color: white; }}
            table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #3498db; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            .footer {{ color: #7f8c8d; font-size: 12px; margin-top: 20px; border-top: 1px solid #ddd; padding-top: 10px; }}
        </style>
    </head>
    <body>
        <h1>Stock Market Daily Report - {report_date}</h1>

        <h2>Alerts ({len(alerts)})</h2>
    """

    if alerts:
        for alert in alerts:
            html += f'<div class="alert {alert["severity"]}">[{alert["severity"]}] {alert["message"]}</div>\n'
    else:
        html += "<p>No alerts for today.</p>"

    html += """
        <h2>Performance</h2>
        <img src="cid:performance" style="width:100%; max-width:700px;">

        <h2>Daily Returns</h2>
        <img src="cid:daily_returns" style="width:100%; max-width:700px;">

        <h2>Volume</h2>
        <img src="cid:volume" style="width:100%; max-width:700px;">

        <h2>Trading Signals (SMA 9/21)</h2>
        <img src="cid:signals" style="width:100%; max-width:700px;">

        <div class="footer">
            <p>Generated automatically by Stock Market ETL Pipeline (Airflow)</p>
            <p>Data source: Yahoo Finance | Indicators: SMA-9, SMA-21, Volatility 30d</p>
        </div>
    </body>
    </html>
    """

    msg = MIMEMultipart("related")
    msg["Subject"] = f"Stock Report - {report_date} | {len(alerts)} alerts"
    msg["From"] = smtp_user
    msg["To"] = email_to

    msg.attach(MIMEText(html, "html"))

    # Attach chart images
    chart_files = {
        "performance": CHARTS_DIR / "performance.png",
        "daily_returns": CHARTS_DIR / "daily_returns.png",
        "volume": CHARTS_DIR / "volume.png",
        "signals": CHARTS_DIR / "signals.png",
    }

    for cid, path in chart_files.items():
        if path.exists():
            with open(path, "rb") as f:
                img = MIMEImage(f.read())
                img.add_header("Content-ID", f"<{cid}>")
                img.add_header("Content-Disposition", "inline", filename=f"{cid}.png")
                msg.attach(img)

    # Send email
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    logger.info("Email report sent to %s", email_to)
