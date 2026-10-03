import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from os.path import join
import pandas as pd
from airflow.macros import ds_add

# CRON Expression: *(min) *(hora) *(diaMês) *(Mês) *(diaSemana), sendo: 0 min, 0 horas, * dia do mês, * mês, 1 dia da semana (segunda-feira)

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule='0 0 * * 1', # Executa toda segunda-feira à meia-noite
) as dag:

    tarefa_1 = BashOperator(
        task_id="cria_pasta",
        bash_command='mkdir -p "/home/paulo/Documentos/AirflowAlura/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_dados(data_interval_end):
        
        city = 'Boston'
        key = 'PVB5587TQEUM5Y3ULK4BLXZ8N'

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                    f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(URL)

        file_path = f'/home/paulo/Documentos/AirflowAlura/semana={data_interval_end}/'

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

    tarefa_2 = PythonOperator(
        task_id="extrai_dados",
        python_callable=extrai_dados,
        op_kwargs= {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2