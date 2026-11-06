"""Checks that run without network access: pytest tests/"""
from pathlib import Path

import pandas as pd
import pytest
from airflow.dag_processing.dagbag import DagBag

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "sample_data" / "semana=2026-09-07" / "dados_brutos.csv"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(ROOT / "dags"))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"meu_primeiro_dag", "atividade_aula_4", "dados_climaticos"}


def test_first_dag_dependencies(dagbag):
    dag = dagbag.dags["meu_primeiro_dag"]
    assert dag.get_task("tarefa_1").downstream_task_ids == {"tarefa_2", "tarefa_3"}
    assert dag.get_task("tarefa_3").downstream_task_ids == {"cria_pasta"}
    # the folder is created with an absolute path
    assert 'mkdir -p "/' in dag.get_task("cria_pasta").bash_command


def test_weather_url_covers_seven_days():
    from dags.dados_climaticos import build_url

    url = build_url("Boston", "2026-09-07", "KEY")
    assert "/Boston/2026-09-07/2026-09-14?" in url
    assert url.endswith("key=KEY&contentType=csv")


def test_weather_files_from_sample(tmp_path):
    from dags.dados_climaticos import salva_arquivos

    salva_arquivos(pd.read_csv(SAMPLE, index_col=0), tmp_path)
    temps = pd.read_csv(tmp_path / "temperaturas.csv", index_col=0)
    assert list(temps.columns) == ["datetime", "tempmin", "temp", "tempmax"]
    assert (temps["tempmin"] <= temps["tempmax"]).all()
    assert (tmp_path / "condicoes.csv").exists()
