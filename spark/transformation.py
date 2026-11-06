"""Bronze -> Silver: split the raw API pages into a `tweet` table and a `user` table.

    spark-submit spark/transformation.py \
        --src  datalake/Bronze/twitter_datascience/extract_date=2026-09-18 \
        --dest datalake/Silver/twitter_datascience \
        --process-date 2026-09-18
"""
import argparse
from os.path import join

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def get_tweets_data(df):
    """One row per tweet, with the public_metrics struct flattened into columns."""
    return df.select(f.explode("data").alias("tweets")).select(
        "tweets.author_id",
        "tweets.conversation_id",
        "tweets.created_at",
        "tweets.id",
        "tweets.public_metrics.*",
        "tweets.text",
    )


def get_users_data(df):
    """One row per user found in includes.users."""
    return df.select(f.explode("includes.users").alias("users")).select("users.*")


def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)


def twitter_transformation(spark, src, dest, process_date):
    """`src` is the Bronze partition folder of the day (extract_date=...)."""
    df = spark.read.json(src)

    table_dest = join(dest, "{table_name}", f"process_date={process_date}")
    export_json(get_tweets_data(df), table_dest.format(table_name="tweet"))
    export_json(get_users_data(df), table_dest.format(table_name="user"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Twitter Transformation")
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("twitter_transformation").getOrCreate()
    twitter_transformation(spark, args.src, args.dest, args.process_date)
    spark.stop()
