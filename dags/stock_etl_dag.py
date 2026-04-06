"""
Stock Market ETL Pipeline

Extracts daily stock prices from Yahoo Finance,
transforms and loads into PostgreSQL data warehouse (star schema).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.extract import extract_stock_data
from scripts.transform import transform_stock_data
from scripts.load import load_dim_company, load_fact_stock_price, load_fact_daily_summary


TICKERS = [
    "PETR4.SA",  # Petrobras
    "VALE3.SA",  # Vale
    "ITUB4.SA",  # Itaú Unibanco
    "BBDC4.SA",  # Bradesco
    "ABEV3.SA",  # Ambev
    "WEGE3.SA",  # WEG
    "RENT3.SA",  # Localiza
    "BBAS3.SA",  # Banco do Brasil
    "SUZB3.SA",  # Suzano
    "GGBR4.SA",  # Gerdau
]

default_args = {
    "owner": "willian",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_etl",
    default_args=default_args,
    description="ETL pipeline for Brazilian stock market data (B3)",
    schedule_interval="0 20 * * 1-5",  # weekdays at 8pm (after market close)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "stocks", "finance"],
) as dag:

    extract = PythonOperator(
        task_id="extract_stock_data",
        python_callable=extract_stock_data,
        op_kwargs={"tickers": TICKERS},
    )

    transform = PythonOperator(
        task_id="transform_stock_data",
        python_callable=transform_stock_data,
    )

    load_companies = PythonOperator(
        task_id="load_dim_company",
        python_callable=load_dim_company,
    )

    load_prices = PythonOperator(
        task_id="load_fact_stock_price",
        python_callable=load_fact_stock_price,
    )

    load_summary = PythonOperator(
        task_id="load_fact_daily_summary",
        python_callable=load_fact_daily_summary,
    )

    extract >> transform >> load_companies >> load_prices >> load_summary
