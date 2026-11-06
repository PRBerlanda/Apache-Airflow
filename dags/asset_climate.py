"""ETL with Airflow Assets: NOAA global temperature anomalies, Celsius -> Fahrenheit.

retrieve  -> downloads the yearly anomaly series (1880-2022) from NOAA
to_fahrenheit -> converts each anomaly (degrees C) to degrees F
load      -> writes the result to <AIRFLOW_DATA_DIR>/climate/fahrenheit.json and
             marks the Asset as updated, so DAGs scheduled on it can start.
"""
import json
import os
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task

SRC_URL = (
    "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/"
    "global/time-series/globe/land_ocean/ytd/12/1880-2022.json"
)
DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", Path.home() / "airflow-data" / "celery"))
OUTPUT = DATA_DIR / "climate" / "fahrenheit.json"

SRC = Asset(SRC_URL)
DEST = Asset(OUTPUT.as_uri())


def anomaly_to_fahrenheit(temps: dict[str, dict[str, float]]) -> dict[str, float]:
    """Convert {"1880": {"anomaly": -0.17}, ...} into {"1880": 31.69, ...}.

    Note: this converts the *anomaly* with the absolute formula (C * 1.8 + 32),
    as in the original Airflow tutorial. An anomaly difference in F would be C * 1.8.
    """
    return {year: float(info["anomaly"]) * 1.8 + 32 for year, info in temps.items()}


@dag(start_date=pendulum.datetime(2026, 9, 22, tz="UTC"), schedule="0 0 * * *", catchup=False)
def etl():
    @task(inlets=[SRC])
    def retrieve() -> dict:
        import requests

        resp = requests.get(SRC_URL, timeout=60)
        resp.raise_for_status()
        return resp.json()["data"]

    @task()
    def to_fahrenheit(temps: dict) -> dict:
        return anomaly_to_fahrenheit(temps)

    @task(outlets=[DEST])
    def load(fahrenheit: dict) -> None:
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(json.dumps(fahrenheit))

    load(to_fahrenheit(retrieve()))


etl()
