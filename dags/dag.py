"""My first DAG: task dependencies with EmptyOperator and BashOperator.

Flow: tarefa_1 >> [tarefa_2, tarefa_3]; tarefa_3 >> cria_pasta
"""
import os
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
# EmptyOperator does nothing; it is only used to shape the task flow.
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

# Where output folders are created. Override with the AIRFLOW_DATA_DIR env var.
DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", Path.home() / "airflow-data" / "basics"))

with DAG(
    "meu_primeiro_dag",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
) as dag:
    tarefa_1 = EmptyOperator(task_id="tarefa_1")
    tarefa_2 = EmptyOperator(task_id="tarefa_2")
    tarefa_3 = EmptyOperator(task_id="tarefa_3")

    tarefa_4 = BashOperator(
        task_id="cria_pasta",
        bash_command=f'mkdir -p "{DATA_DIR}/pasta={{{{ data_interval_end }}}}"',
    )
    # Useful template variables:
    # data_interval_start: start of the data interval
    # data_interval_end: end of the data interval
    # ds: logical date of the run (YYYY-MM-DD)
    # ds_nodash: logical date without dashes (YYYYMMDD)

    tarefa_1 >> [tarefa_2, tarefa_3]
    tarefa_3 >> tarefa_4
