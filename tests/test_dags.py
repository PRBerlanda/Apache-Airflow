"""Checks that run without network access: pytest tests/"""
from pathlib import Path

import pandas as pd
import pytest
from airflow.dag_processing.dagbag import DagBag

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_STOCKS = ROOT / "sample_data" / "stocks"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(ROOT / "dags"))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"get_stocks_dag", "get_crypto_dag", "etl"}


def test_one_task_per_ticker_in_small_pool(dagbag):
    stocks = dagbag.dags["get_stocks_dag"]
    assert set(stocks.task_ids) == {"AAPL", "MSFT", "GOOG", "TSLA"}
    crypto = dagbag.dags["get_crypto_dag"]
    assert set(crypto.task_ids) == {"BTC", "ETH", "DOGE", "AVAX"}
    assert {t.pool for t in stocks.tasks + crypto.tasks} == {"small_pool"}


def test_crypto_uses_usd_pairs():
    from dags.get_crypto import TICKERS

    assert all(t.endswith("-USD") for t in TICKERS.values())


def test_asset_etl_flow(dagbag):
    etl = dagbag.dags["etl"]
    assert etl.get_task("retrieve").downstream_task_ids == {"to_fahrenheit"}
    assert etl.get_task("to_fahrenheit").downstream_task_ids == {"load"}
    assert len(etl.get_task("load").outlets) == 1


def test_fahrenheit_conversion():
    from dags.asset_climate import anomaly_to_fahrenheit

    out = anomaly_to_fahrenheit({"1880": {"anomaly": -0.17}, "2022": {"anomaly": 0.0}})
    assert out == pytest.approx({"1880": 31.694, "2022": 32.0})


@pytest.mark.parametrize("ticker", ["GOOG", "MSFT", "TSLA"])
def test_sample_files_have_ohlc(ticker):
    for csv in sorted((SAMPLE_STOCKS / ticker).glob("*.csv")):
        df = pd.read_csv(csv)
        assert {"Open", "High", "Low", "Close", "Volume"} <= set(df.columns)
        if len(df):
            assert (df["High"] >= df["Low"]).all()
