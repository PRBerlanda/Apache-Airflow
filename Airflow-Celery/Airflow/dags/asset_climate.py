import json
from typing import cast
import requests
from airflow import Asset
from airflow.sdk import dag, task
import pendulum

SRC = Asset(
    "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series/globe/land_ocean/ytd/12/1880-2022.json"
)

@dag(start_date=pendulum.datetime(2026,9,22, tz="UTC"), schedule="0 0 * * *", catchup=False)
def etl():
    @task()
    def retrieve(src: Asset) -> dict:
        resp = requests.get(url=src.uri)
        data = resp.json()
        return data["data"]

    @task()
    def to_fahrenheit(temps: dict[int, dict[str, float]]) -> dict[int, float]:
        ret: dict[int, float] = {}
        for year, info in temps.items():
            ret[year] = float(info["anomaly"]) * 1.8 + 32

        return ret

    @task()
    def load(fahrenheit: dict[int, float]) -> Asset:
        filename = "/tmp/fahrenheit.json"
        s = json.dumps(fahrenheit)
        f = open(filename, "w")
        f.write(s)
        f.close()

        return Asset(f"file:///{filename}")

    data = retrieve(SRC)
    fahrenheit = to_fahrenheit(cast(dict[int, dict[str, float]], data))
    load(cast(dict[int, float], fahrenheit))


etl()