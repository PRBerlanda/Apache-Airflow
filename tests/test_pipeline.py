"""Checks that run without network access (needs Java 17+ for PySpark): pytest tests/

The Spark results are compared with an answer key computed independently with pandas
from the Bronze sample in datalake/.
"""
import json
import sys
from pathlib import Path

import pandas as pd
import pytest
from airflow.dag_processing.dagbag import DagBag

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT / "spark"))
BRONZE = ROOT / "datalake" / "Bronze" / "twitter_datascience" / "extract_date=2026-09-18"

from insight_tweet import twitter_insight  # noqa: E402
from transformation import twitter_transformation  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def answer_key():
    """Expected tweets table computed with plain Python/pandas."""
    tweets = []
    for line in next(BRONZE.glob("*.json")).read_text(encoding="utf-8").splitlines():
        for t in json.loads(line)["data"]:
            tweets.append({"id": t["id"], "created_at": t["created_at"], **t["public_metrics"]})
    df = pd.DataFrame(tweets)
    df["created_date"] = pd.to_datetime(df["created_at"].str[:10]).dt.date
    return df


@pytest.fixture(scope="module")
def lake(tmp_path_factory, spark):
    out = tmp_path_factory.mktemp("lake")
    twitter_transformation(spark, str(BRONZE), str(out / "Silver"), "2026-09-18")
    twitter_insight(spark, str(out / "Silver"), str(out / "Gold"), "2026-09-18")
    return out


def read_lines(folder: Path) -> pd.DataFrame:
    return pd.concat(pd.read_json(p, lines=True, dtype=False) for p in folder.glob("part-*.json"))


def test_dag_structure():
    bag = DagBag(dag_folder=str(ROOT / "dags"))
    assert bag.import_errors == {}
    dag = bag.dags["TwitterDAG"]
    assert dag.get_task("twitter_datascience").downstream_task_ids == {"transform_twitter_datascience"}
    assert dag.get_task("transform_twitter_datascience").downstream_task_ids == {"insight_twitter"}
    for task_id in ("transform_twitter_datascience", "insight_twitter"):
        assert Path(dag.get_task(task_id).application).exists()


def test_silver_tweet_table(lake, answer_key):
    tweet = read_lines(lake / "Silver" / "tweet" / "process_date=2026-09-18")
    assert len(tweet) == len(answer_key)
    assert {"author_id", "id", "like_count", "retweet_count", "text"} <= set(tweet.columns)


def test_silver_user_table(lake):
    user = read_lines(lake / "Silver" / "user" / "process_date=2026-09-18")
    assert {"id", "name", "username", "created_at"} <= set(user.columns)


def test_gold_matches_answer_key(lake, answer_key):
    gold = read_lines(lake / "Gold" / "process_date=2026-09-18")
    gold["created_date"] = pd.to_datetime(gold["created_date"]).dt.date
    gold = gold.set_index("created_date").sort_index()

    expected = answer_key.groupby("created_date").agg(
        n_tweets=("id", "count"),  # the mock API can repeat ids, so count rows
        n_likes=("like_count", "sum"),
        n_quotes=("quote_count", "sum"),
        n_replies=("reply_count", "sum"),
        n_retweets=("retweet_count", "sum"),
    ).sort_index()

    pd.testing.assert_frame_equal(gold[expected.columns], expected, check_dtype=False, check_names=False)
    assert set(gold["weekday"]) <= {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
