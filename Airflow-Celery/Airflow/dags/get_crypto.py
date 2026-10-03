# API Yahoo Finance
import yfinance
# importanto DAGs e Tasks
from airflow.decorators import dag, task
# importando Macros
from airflow.macros import ds_add
# importando Path para trabalhar com os caminhos do sistema.
from pathlib import Path
import pendulum
from time import sleep

TICKERS = ["BTC", "ETH", "DOGE", "AVAX"]

# Iniciando a primeira task. O decorator "@" permite conversão de função python em instância de tarefa PythonOperator.
# O TaskFlow faz a movimentação dos inputs e outputs entre as tasks usando o XCOM. 
# A declaração é feita de forma automática a partir da chamada de função Python.
@task()
def get_history(ticker, ds=None, ds_nodash=None):        
        caminho = f"/home/paulo/Documentos/AirflowLocalCelery/crypto/{ticker}//{ticker}_{ds_nodash}.csv"
        Path(caminho).parent.mkdir(parents=True, exist_ok=True)
        yfinance\
        .Ticker(ticker)\
        .history(
            interval="4h", # Intervalo de horas
            start=ds_add(ds, -1), # Data de início
            end=ds, # Data final
            prepost=True # Permite que sejam trazidos dados da ação fora do horário comercial
            ).to_csv(caminho)
        sleep(10)

@dag(
    schedule="0 0 * * *",
    start_date= pendulum.datetime(2026,9,18, tz="UTC"), 
    catchup=True # Faz com que o Airflow seja executado a partir da data de start_date. False usa a data de hoje (dia da execução).
)
# Define uma função que itera sobre os TICKERS
def get_crypto_dag():
    for ticker in TICKERS:
        get_history.override(task_id=ticker, pool="small_pool")(ticker)
        
dag = get_crypto_dag()