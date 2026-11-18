import os
import logging
import time

from dotenv import load_dotenv

from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    when,
    concat_ws,
    current_timestamp,
    to_timestamp,
    to_date,
    row_number,
    to_json,
    struct,
date_format,
    trim,
)
from pyspark.sql.types import *

load_dotenv()
jar_files = [
    "jars/clickhouse-jdbc-X.X.X-SNAPSHOT-all.jar"
]

# configurations
KAFKA_BOOTSTRAP_SERVERS= "kafka1:29092,kafka2:29092"
WEATHER_TOPIC="airport-validated"

STARTING_OFFSETS="earliest"
CLICKHOUSE_HOST= "clickhouse"
CLICKHOUSE_PORT= 8123
CLICKHOUSE_USER= os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "aviation_streaming")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "airport_weather")

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION", "./spark/checkpoint/clickhouse_weather_load"
)

MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "5000"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"
ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


CATALOG = "clickhouse"
TARGET_TABLE = f"{CATALOG}.{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"

# Redis (live dashboard)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", str(24 * 3600)))
HISTORY_HOURS = int(os.getenv("HISTORY_HOURS", "24"))
HISTORY_SECONDS = HISTORY_HOURS * 3600
REDIS_INDEX_KEY = "weather:airports"
REDIS_CHANNEL = "weather:updates"

AIRPORTS_KEY = "weather:airports"      # sorted set: icao -> latest observation epoch
LATEST_PREFIX = "weather:latest:"      # hash per airport: latest observation
HISTORY_PREFIX = "weather:history:"    # sorted set per airport: JSON observations
REDIS_CHANNEL = "weather:updates"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("clickhouse-weather-loader")


WEATHER_SCHEMA= StructType([
    StructField("airport_icao", StringType(), True),
    StructField("airport_iata", StringType(), True),
    StructField("airport_name", StringType(), True),
    StructField("city", StringType(), True),

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("elevation_m", DoubleType(), True),

    # Kept as a string; converted to a timestamp during validation.
    StructField("observation_time_utc", StringType(), True),

    StructField("temperature_c", DoubleType(), True),
    StructField("relative_humidity_pct", DoubleType(), True),
    StructField("apparent_temperature_c", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("rain_mm", DoubleType(), True),

    # Arrives as 3.0; parsed as a double, cast to int in validation.
    StructField("weather_code", DoubleType(), True),

    StructField("cloud_cover_pct", DoubleType(), True),
    StructField("wind_speed_knots", DoubleType(), True),
    StructField("wind_direction_degrees", DoubleType(), True),
    StructField("wind_gusts_knots", DoubleType(), True),
    StructField("surface_pressure_hpa", DoubleType(), True),
    StructField("visibility_m", DoubleType(), True),

])

#Column order and names must match the ClickHouse table.
TARGET_COLUMNS = [
    "airport_icao",
    "airport_iata",
    "airport_name",
    "city",
    "latitude",
    "longitude",
    "elevation_m",
    "observation_time_utc",
    "temperature_c",
    "relative_humidity_pct",
    "apparent_temperature_c",
    "precipitation_mm",
    "rain_mm",
    "weather_code",
    "cloud_cover_pct",
    "wind_speed_knots",
    "wind_direction_degrees",
    "wind_gusts_knots",
    "surface_pressure_hpa",
    "visibility_m",
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
]
# Fields sent to Redis (Kafka and ingestion bookkeeping stay in ClickHouse).
META_FIELDS = [
    "airport_icao", "airport_iata", "airport_name", "city",
    "latitude", "longitude", "elevation_m",
]
METRIC_FIELDS = [
    "temperature_c", "relative_humidity_pct", "apparent_temperature_c",
    "precipitation_mm", "rain_mm", "weather_code", "cloud_cover_pct",
    "wind_speed_knots", "wind_direction_degrees", "wind_gusts_knots",
    "surface_pressure_hpa", "visibility_m",
]
LATEST_FIELDS = META_FIELDS + ["observation_time_utc"] + METRIC_FIELDS


def create_spark_session()-> SparkSession:
    spark=(
        SparkSession.builder
        .appName("clickhouse-weather-loader")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config(f"spark.sql.catalog.{CATALOG}",
                "com.clickhouse.spark.ClickHouseCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.host", CLICKHOUSE_HOST)
        .config(f"spark.sql.catalog.{CATALOG}.port", str(CLICKHOUSE_PORT))
        .config(f"spark.sql.catalog.{CATALOG}.protocol", "http")
        .config(f"spark.sql.catalog.{CATALOG}.user", CLICKHOUSE_USER)
        .config(f"spark.sql.catalog.{CATALOG}.password", CLICKHOUSE_PASSWORD)
        .config(f"spark.sql.catalog.{CATALOG}.database", CLICKHOUSE_DATABASE)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

#read from kafka
def read_from_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", WEATHER_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

#parse and shape the rows
def build_rows(kafka_df: DataFrame) -> DataFrame:
    """Turn kafka messages intp rows matching the Clickhouse table"""
    parsed=(
        kafka_df
        .selectExpr(
            "CAST(value AS STRING) AS original_json",
            "partition AS kafka_partition",
            "offset AS kafka_offset"
        )
        .withColumn(
            "w", from_json(col("original_json"), WEATHER_SCHEMA)
        )
        .select("kafka_partition","kafka_offset","w.*")
    )

    return (
        parsed
        .withColumn(
            "observation_time_utc",
            to_timestamp(col("observation_time_utc"), TIMESTAMP_FORMAT),
        )
        .withColumn("weather_code", col("weather_code").cast("int"))
        .withColumn("kafka_partition", col("kafka_partition").cast("int"))
        .withColumn("ingested_at", current_timestamp())
        .select(*TARGET_COLUMNS)
    )

#REDIS

def _redis_client():
    # Imported here so it also works on Spark executors.
    import redis

    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
    )

def _as_string(name: str):
    if name == "observation_time_utc":
        return date_format(col(name), ISO_FORMAT).alias(name)
    return col(name).cast("string").alias(name)

def build_latest_redis_df(df: DataFrame) -> DataFrame:
    """One row per airport: its newest observation in this batch."""
    newest_first = Window.partitionBy("airport_icao").orderBy(
        col("observation_time_utc").desc()
    )
    return (
        df.withColumn("rn", row_number().over(newest_first))
        .filter(col("rn") == 1)
        .select(
            col("airport_icao").alias("icao"),
            col("observation_time_utc").cast("long").alias("score"),
            *[_as_string(c) for c in LATEST_FIELDS],
        )
    )


def build_history_df(df: DataFrame) -> DataFrame:
    """One JSON payload per (airport, observation time)."""
    payload_cols = [
                       date_format(col("observation_time_utc"), ISO_FORMAT).alias("observation_time_utc")
                   ] + [col(c) for c in METRIC_FIELDS]

    return df.select(
        col("airport_icao").alias("icao"),
        col("observation_time_utc").cast("long").alias("score"),
        to_json(struct(*payload_cols)).alias("payload"),
    )

def write_latest_partition_to_redis(rows) -> None:
    """Runs on executors: upsert each weather as a hash and index it."""
    client = _redis_client()
    pipe = client.pipeline(transaction=False)
    count = 0

    for row in rows:
        data = row.asDict()
        icao= data.pop("icao")
        score = data.pop("score")
        mapping = {k: v for k, v in data.items() if v is not None}

        key=f"{LATEST_PREFIX}{icao}"
        pipe.delete(key)
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, REDIS_TTL_SECONDS)
        pipe.zadd(REDIS_INDEX_KEY, {icao: score})
        count += 1

    if count:
        pipe.execute()


def write_history_partition(rows) -> None:
    """Runs on executors: append observations and trim old ones."""
    pipe = _redis_client().pipeline(transaction=False)
    count = 0

    for row in rows:
        key = f"{HISTORY_PREFIX}{row['icao']}"
        score = row["score"]

        pipe.zadd(key, {row["payload"]: score})
        pipe.zremrangebyscore(key, "-inf", score - HISTORY_SECONDS)
        pipe.expire(key, REDIS_TTL_SECONDS)
        count += 1

    if count:
        pipe.execute()


def write_to_redis(df: DataFrame, batch_id: int) -> None:
    build_latest_redis_df(df).foreachPartition(write_latest_partition_to_redis)
    build_history_df(df).foreachPartition(write_history_partition)

    client = _redis_client()
    # Forget airports that have not reported within the TTL window.
    client.zremrangebyscore(
        AIRPORTS_KEY, "-inf", time.time() - REDIS_TTL_SECONDS
    )
    # Tell any subscriber there is fresh data.
    client.publish(REDIS_CHANNEL, str(batch_id))

#write each micro-batch to clickhouse
def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()

    try:
        if batch_df.isEmpty():
            logger.info("Batch %s is empty", batch_id)
            return

        # Rows without a key can never be stored, since ORDER BY needs
        # airport_icao and observation_time_utc. The topic is already
        # validated upstream, so any such row is a real fault: log it.
        good_df = batch_df.filter(
            col("airport_icao").isNotNull() &
            col("observation_time_utc").isNotNull()
        )
        total = batch_df.count()
        good = good_df.count()
        if good < total:
            logger.warning(
                "Batch %s: dropping %s rows without airport or time",
                batch_id,
                total - good,
            )

        # Remove duplicates inside this batch. Duplicates across batches
        # (replays, restarts) are collapsed by ReplacingMergeTree.
        deduped_df = good_df.dropDuplicates(
            ["airport_icao", "observation_time_utc"]
        )

        # ClickHouse is the source of truth: if this fails, the batch fails
        # and Spark retries it.
        deduped_df.writeTo(TARGET_TABLE).append()

        # Redis only feeds the live dashboard, so a Redis outage is logged
        # but does not fail the batch (which would re-send it to ClickHouse).
        try:
            write_to_redis(deduped_df, batch_id)
        except Exception:
            logger.exception(
                "Batch %s: Redis write failed, ClickHouse write kept", batch_id
            )

        logger.info(
            "Batch %s: wrote %s rows to %s",
            batch_id,
            deduped_df.count(),
            TARGET_TABLE,
        )

    finally:
        batch_df.unpersist()


# MAIN

def main():
    spark = create_spark_session()

    try:
        rows_df = build_rows(read_from_kafka(spark))

        query = (
            rows_df.writeStream
            .outputMode("append")
            .foreachBatch(write_batch)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .start()
        )

        logger.info(
            "ClickHouse loader started (input=%s, target=%s, "
            "startingOffsets=%s, checkpoint=%s)",
            WEATHER_TOPIC,
            TARGET_TABLE,
            STARTING_OFFSETS,
            CHECKPOINT_LOCATION,
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()