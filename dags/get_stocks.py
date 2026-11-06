"""Daily 4-hour price history for US stocks (Yahoo Finance via yfinance).

One task per ticker, created dynamically with TaskFlow. Each task saves
<AIRFLOW_DATA_DIR>/stocks/<TICKER>/<TICKER>_<YYYYMMDD>.csv
covering the day before the run's logical date (pre- and post-market included).

Tasks run in the pool ``small_pool`` so the Celery workers do not hit
Yahoo Finance with too many parallel requests. Create it before running:
    airflow pools set small_pool 2 "Limits parallel calls to Yahoo Finance"
"""
import os
from pathlib import Path
from time import sleep

import pendulum
from airflow.sdk import dag, task

TICKERS = ["AAPL", "MSFT", "GOOG", "TSLA"]
DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", Path.home() / "airflow-data" / "celery"))


def output_path(ticker: str, ds_nodash: str) -> Path:
    return DATA_DIR / "stocks" / ticker / f"{ticker}_{ds_nodash}.csv"


# @task turns the function into a PythonOperator. TaskFlow passes inputs and
# outputs between tasks through XCom automatically. `ds` and `ds_nodash` are
# filled in from the run context.
@task()
def get_history(ticker, ds=None, ds_nodash=None):
    import yfinance

    caminho = output_path(ticker, ds_nodash)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    start = pendulum.parse(ds).subtract(days=1).to_date_string()
    yfinance.Ticker(ticker).history(
        interval="4h",  # 4-hour candles
        start=start,  # previous day
        end=ds,  # logical date (exclusive)
        prepost=True,  # include pre- and post-market trading
    ).to_csv(caminho)
    sleep(10)  # be gentle with the Yahoo Finance API


@dag(
    # Tuesday to Saturday at 00:00 UTC: each run fetches the previous trading day (Mon-Fri).
    schedule="0 0 * * 2-6",
    start_date=pendulum.datetime(2026, 9, 18, tz="UTC"),
    catchup=True,  # also run every missed interval since start_date
)
def get_stocks_dag():
    for ticker in TICKERS:
        get_history.override(task_id=ticker, pool="small_pool")(ticker)


dag = get_stocks_dag()
