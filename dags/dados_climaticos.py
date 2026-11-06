"""Weekly weather extraction from the Visual Crossing API.

Every Monday at 00:00 UTC the DAG:
  1. creates the folder <AIRFLOW_DATA_DIR>/semana=<YYYY-MM-DD>
  2. downloads the 7-day forecast for Boston and saves three CSV files:
     dados_brutos.csv (raw), temperaturas.csv (temperatures) and condicoes.csv (conditions).

The API key is read at run time from the Airflow Variable ``visual_crossing_key``.
Set it with:  export AIRFLOW_VAR_VISUAL_CROSSING_KEY=<your key>
(free key at https://www.visualcrossing.com/weather-api).
"""
import os
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", Path.home() / "airflow-data" / "basics"))
CITY = "Boston"
BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

# CRON expression: minute hour day-of-month month day-of-week
# "0 0 * * 1" = minute 0, hour 0, any day of month, any month, Monday.


def build_url(city: str, start: str, key: str) -> str:
    end = pendulum.parse(start).add(days=7).to_date_string()
    return (
        f"{BASE_URL}/{city}/{start}/{end}"
        f"?unitGroup=metric&include=days&key={key}&contentType=csv"
    )


def salva_arquivos(dados, folder: Path) -> None:
    """Write the raw table and two subsets (temperatures, conditions)."""
    dados.to_csv(folder / "dados_brutos.csv")
    dados[["datetime", "tempmin", "temp", "tempmax"]].to_csv(folder / "temperaturas.csv")
    dados[["datetime", "description", "icon"]].to_csv(folder / "condicoes.csv")


def extrai_dados(data_interval_end: str) -> None:
    import pandas as pd

    key = Variable.get("visual_crossing_key")
    dados = pd.read_csv(build_url(CITY, data_interval_end, key))
    salva_arquivos(dados, DATA_DIR / f"semana={data_interval_end}")


with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="0 0 * * 1",  # every Monday at midnight (UTC)
    catchup=False,
) as dag:
    tarefa_1 = BashOperator(
        task_id="cria_pasta",
        bash_command=f'mkdir -p "{DATA_DIR}/semana={{{{ data_interval_end.strftime("%Y-%m-%d") }}}}"',
    )

    tarefa_2 = PythonOperator(
        task_id="extrai_dados",
        python_callable=extrai_dados,
        op_kwargs={"data_interval_end": '{{ data_interval_end.strftime("%Y-%m-%d") }}'},
    )

    tarefa_1 >> tarefa_2
