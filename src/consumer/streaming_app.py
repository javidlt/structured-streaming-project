"""Spark Structured Streaming consumer.

Pipeline:
    Kafka(transactions) ──▶ from_json ──▶ filter(status=approved) ──▶ watermark(1 min)
                                                                       │
                                                ┌──────────────────────┴────────────────────────┐
                                                ▼                                                ▼
                                  foreachBatch: raw graph upsert            foreachBatch: 1-min windowed
                                  (Customer)-[:MADE]->(Transaction)         CategoryStats by (category, country)
                                  -[:AT]->(Merchant)

Both sinks talk to Neo4j via the official Spark connector
(``neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3``) using MERGE Cypher
so reruns are idempotent.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

logger = logging.getLogger("consumer")


TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("merchant_id", IntegerType(), nullable=False),
    StructField("transaction_date", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("currency", StringType(), nullable=True),
    StructField("transaction_country", StringType(), nullable=True),
    StructField("transaction_city", StringType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("status", StringType(), nullable=False),
    StructField("device_type", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
])


RAW_GRAPH_CYPHER = """
UNWIND event.batch AS row
MERGE (c:Customer {customer_id: row.customer_id})
MERGE (m:Merchant {merchant_id: row.merchant_id})
  ON CREATE SET m.category = row.merchant_category
MERGE (t:Transaction {transaction_id: row.transaction_id})
  ON CREATE SET
      t.amount = row.amount,
      t.currency = row.currency,
      t.status = row.status,
      t.transaction_date = datetime(row.transaction_date_iso)
MERGE (c)-[r:MADE]->(t)
  ON CREATE SET r.amount = row.amount,
                r.currency = row.currency,
                r.timestamp = datetime(row.transaction_date_iso)
MERGE (t)-[:AT]->(m)
"""


CATEGORY_STATS_CYPHER = """
UNWIND event.batch AS row
MERGE (s:CategoryStats {
    category: row.category,
    country: row.transaction_country,
    window_start: datetime(row.window_start_iso)
})
SET s.window_end = datetime(row.window_end_iso),
    s.total_amount = row.total_amount,
    s.txn_count = row.txn_count,
    s.updated_at = datetime()
"""


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | consumer | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _read_kafka_stream(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def _parse_transactions(raw: DataFrame) -> DataFrame:
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
        .select(F.from_json("json_str", TRANSACTION_SCHEMA).alias("t"), "kafka_ts")
        .select("t.*", "kafka_ts")
        .withColumn("transaction_date_ts", F.to_timestamp("transaction_date"))
    )
    return parsed


def _filter_and_watermark(parsed: DataFrame, watermark: str) -> DataFrame:
    return (
        parsed.where(F.col("status") == F.lit("approved"))
        .withWatermark("transaction_date_ts", watermark)
    )


def _neo4j_writer_options() -> dict[str, str]:
    return {
        "url": os.environ["NEO4J_URI"],
        "authentication.type": "basic",
        "authentication.basic.username": os.environ["NEO4J_USER"],
        "authentication.basic.password": os.environ["NEO4J_PASSWORD"],
        "database": os.environ.get("NEO4J_DATABASE", "neo4j"),
    }


def _write_raw_graph(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink #1: upsert raw transaction subgraph."""
    if batch_df.rdd.isEmpty():
        logger.info("[raw_graph] batch=%d empty", batch_id)
        return

    payload = (
        batch_df.select(
            F.col("transaction_id"),
            F.col("customer_id"),
            F.col("merchant_id"),
            F.col("amount"),
            F.col("currency"),
            F.col("status"),
            F.date_format("transaction_date_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                .alias("transaction_date_iso"),
            F.lit(None).cast(StringType()).alias("merchant_category"),
        )
        .dropDuplicates(["transaction_id"])
    )

    count = payload.count()
    logger.info("[raw_graph] batch=%d rows=%d", batch_id, count)

    (
        payload.write
        .format("org.neo4j.spark.DataSource")
        .mode("Append")
        .options(**_neo4j_writer_options())
        .option("query", RAW_GRAPH_CYPHER)
        .save()
    )


def _write_category_stats(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink #2: 1-minute tumbling windows of category+country totals."""
    if batch_df.rdd.isEmpty():
        logger.info("[category_stats] batch=%d empty", batch_id)
        return

    payload = (
        batch_df
        .withColumn("window_start_iso",
                    F.date_format("window.start", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        .withColumn("window_end_iso",
                    F.date_format("window.end", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        .select(
            "category",
            "transaction_country",
            "window_start_iso",
            "window_end_iso",
            F.col("total_amount").cast(DoubleType()).alias("total_amount"),
            F.col("txn_count").cast(IntegerType()).alias("txn_count"),
        )
    )

    count = payload.count()
    logger.info("[category_stats] batch=%d rows=%d", batch_id, count)

    (
        payload.write
        .format("org.neo4j.spark.DataSource")
        .mode("Append")
        .options(**_neo4j_writer_options())
        .option("query", CATEGORY_STATS_CYPHER)
        .save()
    )


def _join_with_merchant_category(spark: SparkSession, txns: DataFrame) -> DataFrame:
    """Stream-static join to enrich transactions with merchant.category (used by the
    windowed sink). Merchants table is read from Neo4j once when the stream starts.
    """
    merchants = (
        spark.read
        .format("org.neo4j.spark.DataSource")
        .options(**_neo4j_writer_options())
        .option("labels", "Merchant")
        .load()
        .select(
            F.col("merchant_id").cast(IntegerType()).alias("merchant_id"),
            F.col("category").alias("category"),
        )
    )
    return txns.join(F.broadcast(merchants), on="merchant_id", how="left")


def run() -> int:
    _configure_logging()
    load_dotenv()

    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ["KAFKA_TOPIC"]
    app_name = os.environ.get("SPARK_APP_NAME", "streaming-transactions")
    checkpoint_dir = os.environ.get("SPARK_CHECKPOINT_DIR", "./checkpoints")
    watermark = os.environ.get("SPARK_WATERMARK", "1 minute")
    window_duration = os.environ.get("SPARK_WINDOW_DURATION", "1 minute")

    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    raw_ckpt = str(Path(checkpoint_dir) / "raw_graph")
    win_ckpt = str(Path(checkpoint_dir) / "category_stats")

    logger.info(
        "starting Spark streaming app=%s topic=%s watermark=%s window=%s",
        app_name, topic, watermark, window_duration,
    )

    spark = _build_spark(app_name)

    raw = _read_kafka_stream(spark, bootstrap, topic)
    parsed = _parse_transactions(raw)
    approved = _filter_and_watermark(parsed, watermark)

    # Sink 1: raw graph upsert
    raw_query = (
        approved.writeStream
        .queryName("raw_graph_sink")
        .foreachBatch(_write_raw_graph)
        .option("checkpointLocation", raw_ckpt)
        .outputMode("append")
        .start()
    )

    # Sink 2: enrich with merchant.category, then windowed aggregation
    enriched = _join_with_merchant_category(spark, approved)
    windowed = (
        enriched
        .groupBy(
            F.window(F.col("transaction_date_ts"), window_duration),
            F.col("category"),
            F.col("transaction_country"),
        )
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count(F.lit(1)).alias("txn_count"),
        )
        .where(F.col("category").isNotNull())
    )

    win_query = (
        windowed.writeStream
        .queryName("category_stats_sink")
        .foreachBatch(_write_category_stats)
        .option("checkpointLocation", win_ckpt)
        .outputMode("update")
        .start()
    )

    logger.info("streaming queries running: raw=%s, windowed=%s",
                raw_query.name, win_query.name)
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
