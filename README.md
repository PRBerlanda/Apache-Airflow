# Airflow Basics: first DAGs and a weekly weather extraction

Part of the [Apache-Airflow](https://github.com/PRBerlanda/Apache-Airflow) portfolio. Each project lives in its own branch.

This project covers the fundamentals of Apache Airflow 3: what a DAG is, how tasks depend on each other, the main operators, Jinja templates and cron schedules. The last DAG is a small real extraction: every Monday it downloads the 7-day weather forecast for Boston from the Visual Crossing API and saves it as CSV.

## DAGs

| DAG | File | What it shows |
|---|---|---|
| `meu_primeiro_dag` | `dags/dag.py` | Task dependencies (`tarefa_1 >> [tarefa_2, tarefa_3]`) with `EmptyOperator`, and a `BashOperator` that creates a folder named after `data_interval_end` |
| `atividade_aula_4` | `dags/atividade.py` | A single `PythonOperator` |
| `dados_climaticos` | `dags/dados_climaticos.py` | Cron schedule `0 0 * * 1` (Mondays), `BashOperator` + `PythonOperator`, templated `op_kwargs`, API key read from an Airflow Variable |

```
dags/                 DAG files
sample_data/          output of one real run of dados_climaticos (week of 2026-09-07)
tests/test_dags.py    offline checks (pytest)
requirements.txt      pinned dependencies
.env.example          environment variables to copy into .env
```

## How to run

Tested on Linux/WSL with Python 3.13 (the original project used Python 3.14).

```bash
git clone -b airflow-basics https://github.com/PRBerlanda/Apache-Airflow.git airflow-basics
cd airflow-basics
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # then put your Visual Crossing key in .env
set -a; source .env; set +a

airflow standalone            # UI at http://localhost:8080
```

`airflow standalone` prints the admin password and also saves it in `.airflow/simple_auth_manager_passwords.json.generated`.

To run one DAG once from the terminal, without the UI:

```bash
airflow dags test dados_climaticos 2026-09-07
ls output/semana=2026-09-07     # dados_brutos.csv  temperaturas.csv  condicoes.csv
```

You need a free API key from [Visual Crossing](https://www.visualcrossing.com/weather-api) for `dados_climaticos`. The two other DAGs need no key.

## Tests

```bash
pytest tests/
```

The tests need no internet access and no key. They check that:

- all DAG files import without errors;
- the dependencies of `meu_primeiro_dag` are as described above;
- the API URL covers 7 days;
- the CSV files are built from the sample data with the expected columns.

## Sample data

`sample_data/semana=2026-09-07/` holds the three files produced by one real run:

- `dados_brutos.csv`: the full API response;
- `temperaturas.csv`: `datetime`, `tempmin`, `temp`, `tempmax`;
- `condicoes.csv`: `datetime`, `description`, `icon`.

## Changes from the original course code

- **API key removed from the code.** It is now read at run time from the Airflow Variable `visual_crossing_key`, set through the environment variable `AIRFLOW_VAR_VISUAL_CROSSING_KEY`.
- **Output paths are configurable.** `AIRFLOW_DATA_DIR` replaces the fixed `/home/paulo/...` paths.
- **Fixed a relative path.** `dag.py` called `mkdir -p "home/paulo/..."`, without the leading `/`, so the folder was created relative to wherever the worker happened to run.
- **Airflow 3 imports.** The code now uses `airflow.sdk.DAG` and `airflow.providers.standard.operators.*` instead of the deprecated `airflow.operators.*`.
- **Simpler date and URL code.** The end date (+7 days) is computed with `pendulum`, and the URL is built with an f-string instead of `os.path.join`.
- **`catchup=False`, set explicitly.**
