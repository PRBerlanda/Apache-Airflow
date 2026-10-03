import pendulum
# Directed Aciclic Graph (DAG) definition
from airflow.models import DAG

# Utilizado para organizar o fluxo de tarefas no DAG.
from airflow.operators.empty import EmptyOperator
# Responsável por executar comandos bash no sistema operacional.
from airflow.operators.bash import BashOperator

with DAG(
    'meu_primeiro_dag',
    start_date = pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule='@daily'
) as dag:
    tarefa_1 = EmptyOperator(task_id = 'tarefa_1')
    tarefa_2 = EmptyOperator(task_id = 'tarefa_2')
    tarefa_3 = EmptyOperator(task_id = 'tarefa_3')

    tarefa_4 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "home/paulo/Documentos/AirflowAlura/pasta={{data_interval_end}}"'
    )
    # data_interval_start: data do início do intervalo de dados;
    # data_interval_end: data do fim do intervalo de dados;
    # ds: data lógica de execução do DAG;
    # ds_nodash: data lógica de execução do DAG sem nenhuma separação por traços.

    tarefa_1 >> [tarefa_2, tarefa_3]
    tarefa_3 >> tarefa_4