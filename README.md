# Airflow + Spark: Twitter data lake (Bronze → Silver → Gold)

Part of the [Apache-Airflow](https://github.com/PRBerlanda/Apache-Airflow) portfolio. Each project lives in its own branch.

This project continues [`airflow-pipelines`](https://github.com/PRBerlanda/Apache-Airflow/tree/airflow-pipelines). Airflow extracts tweets about "datascience" from the course's mock Twitter API (`https://labdados.com`, fictitious tweets, no credentials needed). It then calls **Spark** through `SparkSubmitOperator` to build a local data lake in three layers:

| Task | Code | Input → Output |
|---|---|---|
| `twitter_datascience` | `operators/twitter_operator.py` + `hook/twitter_hook.py` | API → `datalake/Bronze/twitter_datascience/extract_date=<ds>/` (raw JSON pages) |
| `transform_twitter_datascience` | `spark/transformation.py` | Bronze → `datalake/Silver/twitter_datascience/{tweet,user}/process_date=<ds>/` |
| `insight_twitter` | `spark/insight_tweet.py` | Silver `tweet` → `datalake/Gold/twitter_datascience/process_date=<ds>/` |

- **Silver:**
  - `tweet` holds one row per tweet, with `public_metrics` flattened into `like_count`, `quote_count`, `reply_count` and `retweet_count`.
  - `user` holds one row per user.
- **Gold:** one row per day with these columns:
  - `n_tweets`;
  - the sums of likes, quotes, replies and retweets;
  - `weekday`.

```
dags/twitter_dag.py        the DAG (TwitterDAG)
hook/, operators/          custom Hook and Operator
spark/                     Spark jobs (spark-submit)
scripts/                   stand-alone extraction script
notebooks/                 exploration (Spark) and a chart of the Gold table (pandas)
datalake/                  sample output of one full run (2026-09-18)
tests/test_pipeline.py     offline checks, Spark results compared with a pandas answer key
```

## How to run

Tested on Linux/WSL with Python 3.13 and Java 21 (the original project used Python 3.14). Spark needs Java 17 or later: `sudo apt install openjdk-21-jre-headless`.

```bash
git clone -b airflow-spark https://github.com/PRBerlanda/Apache-Airflow.git airflow-spark
cd airflow-spark
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
set -a; source .env; set +a

airflow dags test TwitterDAG 2026-09-18
```

Use `airflow standalone` to open the UI at http://localhost:8080.

The `pyspark` package already includes `spark-submit`, and `.env.example` points `SPARK_HOME` to it. If you prefer the full Spark distribution, download it and set `SPARK_HOME` to that folder instead:

```bash
wget https://archive.apache.org/dist/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz
tar -xzf spark-4.2.0-bin-hadoop3.tgz
```

You can also run the Spark jobs without Airflow:

```bash
$SPARK_HOME/bin/spark-submit spark/transformation.py \
  --src datalake/Bronze/twitter_datascience/extract_date=2026-09-18 \
  --dest datalake/Silver/twitter_datascience --process-date 2026-09-18
$SPARK_HOME/bin/spark-submit spark/insight_tweet.py \
  --src datalake/Silver/twitter_datascience \
  --dest datalake/Gold/twitter_datascience --process-date 2026-09-18
```

## Tests

```bash
pytest tests/
```

The tests run the two Spark jobs on the Bronze sample. They then compare the Gold table with an **answer key computed independently in pandas** (tweet count and sums per day). They also check:

- the DAG structure;
- that both Spark scripts exist at the paths the DAG uses.

No internet access is needed.

## Changes from the original course code

- **`n_tweets` metric fixed.** It counted distinct **authors** (`count_distinct("author_id")`); it now counts tweets (`count("id")`).
  - Rows are counted rather than distinct ids because the mock API generates random ids and sometimes repeats them for different tweets. The 2026-09-18 sample has 10 tweets but only 9 distinct ids.
  - Counting rows keeps `n_tweets` consistent with the sums of likes and retweets.
- **Gold now reads only the day's Silver partition** (`process_date=<ds>`).
  - Before, every run re-aggregated the whole Silver history and saved it under the current date, so each Gold partition repeated all previous days.
- **Fixed absolute paths replaced.**
  - `/home/paulo/Documentos/...` is now derived from the project folder, `AIRFLOW_DATA_DIR` and `SPARK_HOME`.
  - The DAG no longer overrides `AIRFLOW_HOME` inside the Spark tasks.
- **Removed an unused variable** (`partition_path`) in `transformation.py`. The Spark session is now stopped at the end of each job.
- **Notebooks fixed so they run from `notebooks/`.**
  - Relative paths replace the absolute ones. The `silver` folder name was corrected to `Silver`; Linux paths are case-sensitive.
  - `import matplotlib as plt` became `import matplotlib.pyplot as plt`.
  - The Gold output is read as a folder of `part-*.json` files.
  - Outputs were cleared.
- **The Twitter hook and operator** received the same fixes as in `airflow-pipelines`.
- **Smaller fixes:** the start date is timezone-aware; Airflow 3 imports.
