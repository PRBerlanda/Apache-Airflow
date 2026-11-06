"""Daily extraction of tweets about "datascience" into a local data lake.

Output: <AIRFLOW_DATA_DIR>/datalake/twitter_datascience/extract_date=<YYYY-MM-DD>/datascience_<YYYYMMDD>.json
Each run asks for the tweets of the previous day: [ds - 1 day, ds).
"""
import os
import sys
from pathlib import Path

import pendulum
from airflow.sdk import DAG

# Make the project root (hook/, operators/) importable from the dags folder.
sys.path.append(str(Path(__file__).resolve().parent.parent))

from operators.twitter_operator import TwitterOperator  # noqa: E402

DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", Path.home() / "airflow-data" / "pipelines"))

with DAG(
    dag_id="TwitterTest",
    start_date=pendulum.datetime(2026, 9, 11, tz="UTC"),
    schedule="@daily",
    catchup=False,
) as dag:
    to = TwitterOperator(
        task_id="test_run",
        file_path=str(
            DATA_DIR / "datalake" / "twitter_datascience"
            / "extract_date={{ ds }}" / "datascience_{{ ds_nodash }}.json"
        ),
        query="datascience",
        start_time="{{ macros.ds_add(ds, -1) }}T00:00:00.00Z",
        end_time="{{ ds }}T00:00:00.00Z",
    )
