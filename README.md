# Apache Airflow – data pipeline projects

Projects built while studying **Apache Airflow 3** (Alura course track). They cover first DAGs, distributed execution with Celery and with Kubernetes, custom hooks and operators, and a Spark data lake.

**Each project lives in its own branch.** This `main` branch only holds this index.

| Branch | Project | Highlights |
|---|---|---|
| [`airflow-basics`](https://github.com/PRBerlanda/Apache-Airflow/tree/airflow-basics) | First DAGs + weekly weather extraction | Operators, task dependencies, Jinja templates, cron schedules, Airflow Variables |
| [`airflow-pipelines`](https://github.com/PRBerlanda/Apache-Airflow/tree/airflow-pipelines) | Twitter-like API extraction | Custom `HttpHook` and `BaseOperator`, pagination, partitioned data lake |
| [`airflow-spark`](https://github.com/PRBerlanda/Apache-Airflow/tree/airflow-spark) | Bronze → Silver → Gold with Spark | `SparkSubmitOperator`, PySpark transformations, analysis notebooks |
| [`airflow-celery`](https://github.com/PRBerlanda/Apache-Airflow/tree/airflow-celery) | Stocks, crypto and climate data | CeleryExecutor (Redis + PostgreSQL), TaskFlow API, dynamic tasks, pools, Assets |
| [`airflow-kubernetes`](https://github.com/PRBerlanda/Apache-Airflow/tree/airflow-kubernetes) | Airflow on minikube | KubernetesExecutor, Helm chart, PersistentVolumes, custom Docker image |

## Reproducing a project

Every branch is self-contained and includes:

- `README.md`: what the project does, step-by-step setup, and the changes made to the original course code;
- `requirements.txt`: pinned dependencies (virtual environments are not committed);
- `.env.example`: the environment variables to set; no secrets are stored in the code;
- `tests/`: offline checks with `pytest`, which need no internet access or API keys;
- sample data from real runs, so you can compare your output.

```bash
git clone -b airflow-spark https://github.com/PRBerlanda/Apache-Airflow.git
cd Apache-Airflow
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
pytest tests/
```

## Stack

Apache Airflow 3.3 · Python 3.13/3.14 · PySpark 4.2 · pandas · yfinance · Celery · Redis · PostgreSQL · Docker · Kubernetes (minikube) · Helm.

The projects were developed on Ubuntu (WSL).
