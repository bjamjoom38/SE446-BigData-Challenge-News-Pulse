import time
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_FILE = Path("data/newsfeed.xml")

STOP_WORDS = [
    "the", "a", "an", "and", "or", "in", "on", "of", "to", "for", "with",
    "is", "are", "as", "after", "this", "that", "by", "from", "at", "it",
    "be", "has", "have", "not", "over", "about", "why", "how", "what",
    "says", "will", "its", "into", "than", "but", "just", "up", "no",
]


def get_text(parent, tag, default=""):
    element = parent.find(tag)
    if element is None or element.text is None:
        return default
    return element.text.strip()


def load_news_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    channel = root.find("channel")
    if channel is None:
        raise ValueError("Invalid XML file: missing channel element")

    source = get_text(channel, "title", "Unknown Source")
    records = []

    for item in channel.findall("item"):
        record = {
            "title": get_text(item, "title"),
            "source": source,
            "published_at": get_text(item, "pubDate"),
            "link": get_text(item, "link"),
            "ingested_at": datetime.now().isoformat(timespec="seconds"),
        }

        if record["title"]:
            records.append(record)

    return records


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("NewsPulse")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def simulate_streaming_batches(records, batch_size=5, delay_seconds=2):
    for batch_number, start in enumerate(range(0, len(records), batch_size), start=1):
        batch = records[start:start + batch_size]

        print(f"\n--- Batch {batch_number} ---")
        print(f"Processing {len(batch)} records")

        yield batch

        time.sleep(delay_seconds)


def build_words_df(news_df):
    return (
        news_df
        .select(F.explode(F.split(F.lower(F.col("title")), " ")).alias("word"))
        .withColumn("word", F.regexp_replace(F.col("word"), "[^a-z0-9]", ""))
        .filter(F.col("word") != "")
        .filter(~F.col("word").isin(STOP_WORDS))
    )


def show_source_counts(news_df, heading):
    print(f"\n{heading}")
    news_df.groupBy("source").count().orderBy(F.desc("count")).show(truncate=False)


def show_trending_keywords(news_df, heading, limit=10):
    print(f"\n{heading}")
    (
        build_words_df(news_df)
        .groupBy("word")
        .count()
        .orderBy(F.desc("count"), F.asc("word"))
        .show(limit, truncate=False)
    )


def process_batch(batch_df):
    print("\nIncoming headlines:")
    batch_df.select("title", "source", "published_at").show(truncate=False)

    show_source_counts(batch_df, "Headlines by source:")
    show_trending_keywords(batch_df, "Top trending keywords:")


def show_final_summary(all_news_df):
    print("\n=== Final News Pulse Summary ===")
    print(f"Total headlines processed: {all_news_df.count()}")

    show_source_counts(all_news_df, "Final headlines by source:")
    show_trending_keywords(all_news_df, "Final top trending keywords:", limit=15)

    print("\nLatest headline sample:")
    all_news_df.select("title", "source", "published_at").show(5, truncate=False)


if __name__ == "__main__":
    spark = create_spark_session()
    news_records = load_news_from_xml(DATA_FILE)
    all_news_df = None

    print(f"Loaded {len(news_records)} news records")

    for batch in simulate_streaming_batches(news_records, batch_size=5, delay_seconds=2):
        batch_df = spark.createDataFrame(batch)
        all_news_df = batch_df if all_news_df is None else all_news_df.unionByName(batch_df)
        process_batch(batch_df)

    if all_news_df is not None:
        show_final_summary(all_news_df)

    spark.stop()
