from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    'atividade_aula_4',
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule='@daily'
) as dag:
    def cumprimentos():
        print("Boas-vindas ao Airflow!")

    tarefa = PythonOperator(
        task_id='cumprimentos',
        python_callable=cumprimentos
    )

    tarefa