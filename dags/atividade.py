"""Exercise: a single PythonOperator that prints a greeting."""
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def cumprimentos():
    print("Boas-vindas ao Airflow!")  # "Welcome to Airflow!"


with DAG(
    "atividade_aula_4",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
) as dag:
    tarefa = PythonOperator(task_id="cumprimentos", python_callable=cumprimentos)
