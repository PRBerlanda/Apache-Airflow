"""Twitter pipeline with a medallion data lake (Bronze -> Silver -> Gold).

1. twitter_datascience (TwitterOperator): raw API pages -> Bronze
   datalake/Bronze/twitter_datascience/extract_date=<ds>/datascience_<ds_nodash>.json
2. transform_twitter_datascience (Spark): Bronze -> Silver, split into `tweet` and `user` tables
   datalake/Silver/twitter_datascience/{tweet,user}/process_date=<ds>/
3. insight_twitter (Spark): Silver -> Gold, daily engagement summary
   datalake/Gold/twitter_datascience/process_date=<ds>/

Environment variables (see README):
  AIRFLOW_DATA_DIR  root of the data lake (default: this project folder)
  SPARK_HOME        Spark installation used by spark-submit
"""
import os
import sys
from pathlib import Path

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_DIR))

from operators.twitter_operator import TwitterOperator  # noqa: E402

DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", PROJECT_DIR))
SPARK_DIR = PROJECT_DIR / "spark"
BASE_FOLDER = str(DATA_DIR / "datalake" / "{stage}" / "twitter_datascience" / "{partition}")
PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"
SPARK_ENV = {"SPARK_HOME": os.environ.get("SPARK_HOME", "")}

with DAG(
    dag_id="TwitterDAG",
    start_date=pendulum.datetime(2026, 9, 17, tz="UTC"),
    schedule="@daily",
    catchup=False,
) as dag:
    # Extraction -> Bronze
    twitter_operator = TwitterOperator(
        task_id="twitter_datascience",
        file_path=os.path.join(
            BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),
            "datascience_{{ ds_nodash }}.json",
        ),
        query="datascience",
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
    )

    # Bronze -> Silver (requires apache-airflow-providers-apache-spark)
    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_datascience",
        application=str(SPARK_DIR / "transformation.py"),
        name="twitter_transformation",
        env_vars=SPARK_ENV,
        application_args=[
            "--src", BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),
            "--dest", BASE_FOLDER.format(stage="Silver", partition=""),
            "--process-date", "{{ ds }}",
        ],
    )

    # Silver -> Gold
    twitter_insight = SparkSubmitOperator(
        task_id="insight_twitter",
        application=str(SPARK_DIR / "insight_tweet.py"),
        name="insight_twitter",
        env_vars=SPARK_ENV,
        application_args=[
            "--src", BASE_FOLDER.format(stage="Silver", partition=""),
            "--dest", BASE_FOLDER.format(stage="Gold", partition=""),
            "--process-date", "{{ ds }}",
        ],
    )

    twitter_operator >> twitter_transform >> twitter_insight
