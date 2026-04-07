"""
Stock Market Analytics & Reporting Pipeline

Runs after the ETL DAG, calculates technical indicators,
generates alerts, charts, and sends a daily email report.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.analytics import (
    calculate_moving_averages,
    calculate_volatility,
    calculate_returns,
    generate_alerts,
)
from scripts.report import generate_charts, send_email_report


default_args = {
    "owner": "willian",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="stock_market_analytics",
    default_args=default_args,
    description="Analytics, alerts and daily email report for stock data",
    schedule_interval="30 20 * * 1-5",  # 30min after ETL DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "stocks", "finance", "email"],
) as dag:

    moving_averages = PythonOperator(
        task_id="calculate_moving_averages",
        python_callable=calculate_moving_averages,
    )

    volatility = PythonOperator(
        task_id="calculate_volatility",
        python_callable=calculate_volatility,
    )

    returns = PythonOperator(
        task_id="calculate_returns",
        python_callable=calculate_returns,
    )

    alerts = PythonOperator(
        task_id="generate_alerts",
        python_callable=generate_alerts,
    )

    charts = PythonOperator(
        task_id="generate_charts",
        python_callable=generate_charts,
    )

    email = PythonOperator(
        task_id="send_email_report",
        python_callable=send_email_report,
    )

    # Indicators run in parallel, then alerts, then charts + email
    [moving_averages, volatility, returns] >> alerts >> charts >> email
