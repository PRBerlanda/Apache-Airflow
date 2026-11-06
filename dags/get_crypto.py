"""Daily 4-hour price history for cryptocurrencies (Yahoo Finance via yfinance).

Yahoo Finance quotes crypto as <SYMBOL>-USD (e.g. BTC-USD). The plain symbols
"BTC"/"ETH" are ETFs listed in the US, not the coins, and "DOGE"/"AVAX" return nothing.

Each task saves <AIRFLOW_DATA_DIR>/crypto/<SYMBOL>/<SYMBOL>_<YYYYMMDD>.csv.
Crypto trades 24/7, so the DAG runs every day. Uses the pool ``small_pool``
(see get_stocks.py).
"""
import os
from pathlib import Path
from time import sleep

import pendulum
from airflow.sdk import dag, task

# Folder/task name -> Yahoo Finance ticker
TICKERS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "DOGE": "DOGE-USD", "AVAX": "AVAX-USD"}
DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", Path.home() / "airflow-data" / "celery"))


def output_path(symbol: str, ds_nodash: str) -> Path:
    return DATA_DIR / "crypto" / symbol / f"{symbol}_{ds_nodash}.csv"


@task()
def get_history(symbol, yahoo_ticker, ds=None, ds_nodash=None):
    import yfinance

    caminho = output_path(symbol, ds_nodash)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    start = pendulum.parse(ds).subtract(days=1).to_date_string()
    yfinance.Ticker(yahoo_ticker).history(
        interval="4h",
        start=start,
        end=ds,
    ).to_csv(caminho)
    sleep(10)


@dag(
    schedule="0 0 * * *",  # every day at 00:00 UTC
    start_date=pendulum.datetime(2026, 9, 18, tz="UTC"),
    catchup=True,
)
def get_crypto_dag():
    for symbol, yahoo_ticker in TICKERS.items():
        get_history.override(task_id=symbol, pool="small_pool")(symbol, yahoo_ticker)


dag = get_crypto_dag()
