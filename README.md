# Airflow with CeleryExecutor: stock, crypto and climate data

Part of the [Apache-Airflow](https://github.com/PRBerlanda/Apache-Airflow) portfolio. Each project lives in its own branch.

This project runs Airflow 3 locally with the **CeleryExecutor**:

- tasks are queued in **Redis**;
- tasks are executed by Celery workers;
- metadata is stored in **PostgreSQL**.

The DAGs use the **TaskFlow API** (`@dag`, `@task`) and create one task per ticker dynamically. A **pool** (`small_pool`) caps how many tasks call Yahoo Finance at the same time. There is also an ETL that uses **Assets**.

## DAGs

| DAG | File | Schedule | Output (`$AIRFLOW_DATA_DIR/...`) |
|---|---|---|---|
| `get_stocks_dag` | `dags/get_stocks.py` | `0 0 * * 2-6` (Tue–Sat 00:00 UTC; each run fetches the previous trading day) | `stocks/<TICKER>/<TICKER>_<YYYYMMDD>.csv` for AAPL, MSFT, GOOG, TSLA |
| `get_crypto_dag` | `dags/get_crypto.py` | daily | `crypto/<SYMBOL>/<SYMBOL>_<YYYYMMDD>.csv` for BTC, ETH, DOGE, AVAX (quoted in USD) |
| `etl` | `dags/asset_climate.py` | daily | `climate/fahrenheit.json`, built from the NOAA global temperature anomalies |

Each stock and crypto task downloads 4-hour candles (`Open, High, Low, Close, Volume, ...`) for one ticker.

```
dags/                     DAG files
notebooks/yfinance.ipynb  exploration of yfinance before writing the DAG
sample_data/stocks/       CSV files produced by real DAG runs (GOOG, MSFT, TSLA)
sample_data/notebook/     hourly AAPL and GOOG history saved by the notebook
tests/test_dags.py        offline checks (pytest)
requirements.txt          pinned dependencies
.env.example              Celery/Postgres/Redis settings
```

## How to run

Tested on Linux/WSL with Python 3.13 (the original project used Python 3.14).

### 1. Redis and PostgreSQL

```bash
sudo apt install redis-server postgresql
sudo service redis-server start
sudo service postgresql start
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'airflow';" \
                      -c "CREATE DATABASE airflow_db OWNER airflow;"
```

### 2. Airflow

```bash
git clone -b airflow-celery https://github.com/PRBerlanda/Apache-Airflow.git airflow-celery
cd airflow-celery
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
set -a; source .env; set +a

airflow db migrate
airflow pools set small_pool 2 "Limits parallel calls to Yahoo Finance"
```

### 3. Start the components

Start each component in its own terminal. Load `.env` in every terminal first.

```bash
airflow api-server        # UI at http://localhost:8080
airflow scheduler
airflow dag-processor
airflow celery worker
airflow celery flower     # optional: Celery monitoring at http://localhost:5555
```

To run one DAG once, without the scheduler:

```bash
airflow dags test get_stocks_dag 2026-09-22
```

## Tests

```bash
pytest tests/
```

The tests need no internet access, Redis or Postgres. They check that:

- every DAG file imports without errors;
- there is one task per ticker, all of them in `small_pool`;
- the crypto tickers are `-USD` pairs;
- the Asset ETL goes `retrieve` → `to_fahrenheit` → `load`, and `load` publishes the Asset;
- the Fahrenheit conversion is correct;
- the sample files have OHLC columns with `High >= Low`.

## Sample data

`sample_data/stocks/` contains the files written by the original runs for GOOG, MSFT and TSLA.

- **Empty files are expected.** Files with only a header are days with no trading, such as the run on 2026-09-21, which covers Sunday 2026-09-20.
- **Some outputs are not included.** The original AAPL and crypto outputs are left out because they came from the wrong tickers; see the changes below.

## Changes from the original course code

- **Stock ticker fixed:** `"APPL"` → `"AAPL"`. Yahoo Finance has no `APPL` ticker, so every AAPL file came out empty.
- **Crypto tickers fixed:** `"BTC"`, `"ETH"`, `"DOGE"`, `"AVAX"` → `"BTC-USD"`, `"ETH-USD"`, `"DOGE-USD"`, `"AVAX-USD"`.
  - On Yahoo Finance, `BTC` and `ETH` are US-listed ETFs, not the coins. The old files showed prices around $33 with a "Capital Gains" column.
  - `DOGE` and `AVAX` returned nothing.
  - Folder and task names stay `BTC`, `ETH`, and so on.
- **Crypto no longer uses `prepost=True`.** Crypto trades 24/7, so there is no pre-market or after-hours session.
- **Asset ETL brought in line with Airflow 3.**
  - Assets are now declared with `inlets`/`outlets`.
  - It imports `from airflow.sdk import Asset`; `airflow.Asset` is deprecated.
  - The year keys are kept as strings, which is what JSON and XCom return.
  - The request has a timeout and checks the HTTP status.
  - Output goes to `$AIRFLOW_DATA_DIR/climate/` instead of `/tmp`.
- **Output paths are configurable.** `AIRFLOW_DATA_DIR` replaces `/home/paulo/...`, and the double slash `//` in the paths was removed.
- **Airflow 3 imports.** The code uses `airflow.sdk.dag` and `airflow.sdk.task` instead of `airflow.decorators`. The start date is computed with `pendulum` instead of `airflow.macros.ds_add`.
