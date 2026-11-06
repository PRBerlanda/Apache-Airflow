# Airflow Pipelines: custom Hook and Operator for a Twitter-like API

Part of the [Apache-Airflow](https://github.com/PRBerlanda/Apache-Airflow) portfolio. Each project lives in its own branch.

This project builds an extraction pipeline step by step:

1. `scripts/extract_API_Twitter.py` is a plain Python script that calls the API and follows the pagination.
2. `hook/twitter_hook.py` (`TwitterHook`) moves that logic into an Airflow `HttpHook`. The base URL comes from an Airflow connection.
3. `operators/twitter_operator.py` (`TwitterOperator`) is a custom operator with templated fields. It saves one JSON page per line.
4. `dags/twitter_dag.py` is a daily DAG that writes `datalake/twitter_datascience/extract_date=<ds>/datascience_<ds_nodash>.json`.

The API is the course's public **mock of the Twitter v2 "recent search" endpoint** (`https://labdados.com`). It returns fictitious tweets, so no Twitter/X account or token is needed.

The [`airflow-spark`](https://github.com/PRBerlanda/Apache-Airflow/tree/airflow-spark) branch continues this project with Spark transformations.

```
dags/twitter_dag.py
hook/twitter_hook.py
operators/twitter_operator.py
scripts/extract_API_Twitter.py
sample_data/extract_twitter.json   output of the stand-alone script
sample_data/datalake/              output of one DAG run (2026-09-17)
tests/test_pipeline.py             offline checks (pytest)
```

## How to run

Tested on Linux/WSL with Python 3.13 (the original project used Python 3.14).

```bash
git clone -b airflow-pipelines https://github.com/PRBerlanda/Apache-Airflow.git airflow-pipelines
cd airflow-pipelines
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
set -a; source .env; set +a

airflow dags test TwitterTest 2026-09-17
cat datalake/twitter_datascience/extract_date=2026-09-17/datascience_20260917.json
```

Use `airflow standalone` to open the UI at http://localhost:8080.

You can also run each step on its own:

```bash
python scripts/extract_API_Twitter.py   # plain requests
python hook/twitter_hook.py             # the hook (needs AIRFLOW_CONN_TWITTER_DEFAULT)
python operators/twitter_operator.py    # the operator, writes ./datalake/...
```

## Tests

```bash
pytest tests/
```

The tests need no internet access. They check:

- the DAG imports cleanly and its output path is absolute and partitioned by `extract_date`;
- the URL the hook builds;
- that pagination follows `next_token`;
- that the operator writes one JSON document per line, using the sample data.

## Changes from the original course code

- **Partition name typo fixed:** `extract_data=` → `extract_date=`.
- **Output path is absolute.** The DAG wrote to the relative path `datalake/...`, which depends on the folder Airflow was started from. It now writes under `$AIRFLOW_DATA_DIR`.
- **Removed `sys.path.append("Airflowpipelines")` from the operator.** That path only existed on the original machine; the project root is now added from the file's own location.
- **The operator's manual test no longer builds a `TaskInstance`.** It calls `execute()` directly; the old way does not work in Airflow 3.
- **Timestamps in UTC (`Z`).** The script used the machine's time zone name, which breaks on Windows and outside UTC-3.
- **Smaller fixes:**
  - `BaseOperator` is imported from `airflow.sdk` (Airflow 3).
  - The start date is timezone-aware.
  - Requests have timeouts.
  - The hook keeps a 100-page safety limit on pagination.
