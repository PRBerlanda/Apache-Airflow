"""Silver -> Gold: daily engagement summary of the tweets.

Output columns: created_date, n_tweets, n_likes, n_quotes, n_replies, n_retweets, weekday

    spark-submit spark/insight_tweet.py \
        --src  datalake/Silver/twitter_datascience \
        --dest datalake/Gold/twitter_datascience \
        --process-date 2026-09-18
"""
import argparse
from os.path import join

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def get_tweet_conversas(df_tweet):
    return (
        df_tweet.alias("tweet")
        .groupBy(f.to_date("created_at").alias("created_date"))
        .agg(
            f.count("id").alias("n_tweets"),  # number of tweets (rows), not distinct authors
            f.sum("like_count").alias("n_likes"),
            f.sum("quote_count").alias("n_quotes"),
            f.sum("reply_count").alias("n_replies"),
            f.sum("retweet_count").alias("n_retweets"),
        )
        .withColumn("weekday", f.date_format("created_date", "E"))
    )


def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)


def twitter_insight(spark, src, dest, process_date):
    """Reads the Silver `tweet` table of `process_date` and writes the Gold summary."""
    df_tweet = spark.read.json(join(src, "tweet", f"process_date={process_date}"))
    export_json(get_tweet_conversas(df_tweet), join(dest, f"process_date={process_date}"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Twitter Insight")
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("twitter_insight").getOrCreate()
    twitter_insight(spark, args.src, args.dest, args.process_date)
    spark.stop()
