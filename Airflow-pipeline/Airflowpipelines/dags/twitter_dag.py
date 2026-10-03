from pathlib import Path
import sys
root_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(root_dir))

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from os.path import join   

with DAG(
    dag_id = "TwitterTest", 
    start_date=datetime(2026, 9, 11, 0, 0, 0),
    schedule="@daily",
    catchup=False) as dag:

        query = "datascience"

        to = TwitterOperator(file_path=join("datalake/twitter_datascience",
            "extract_data={{ ds }}",
            "datascience_{{ ds_nodash }}.json"),
            query=query, start_time="{{ macros.ds_add(ds, -1) }}T00:00:00.00Z", end_time="{{ ds }}T00:00:00.00Z", task_id="test_run")