import os
import time
import logging

from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    from_json,
    lit,
    current_timestamp,
    to_timestamp,
    to_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

load_dotenv()

# configurations
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29092"
FLIGHTS_TOPIC = "aiport-flights-validated"

STARTING_OFFSETS = "earliest"
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "aviation_streaming")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "airport_flights")

# Must differ from the weather loader's checkpoint, or the two jobs will clash.
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION", "./spark/checkpoint/clickhouse_flights_load"
)

MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "5000"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"
DATE_FORMAT = "yyyy-MM-dd"

# Redis (live dashboard)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", str(48 * 3600)))
REDIS_INDEX_KEY = "flights:index"      # sorted set: flight key -> dep_scheduled epoch
REDIS_CHANNEL = "flights:updates"      # pub/sub: one message per batch

CATALOG = "clickhouse"
TARGET_TABLE = f"{CATALOG}.{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("clickhouse-flights-loader")


AIRPORT_SCHEMA = StructType([
    StructField("airport", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("scheduled", StringType(), True),
    StructField("estimated", StringType(), True),
    StructField("actual", StringType(), True),
])

AIRLINE_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
])

FLIGHT_NUMBER_SCHEMA = StructType([
    StructField("number", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
])

FLIGHT_SCHEMA = StructType([
    StructField("flight_date", StringType(), True),
    StructField("flight_status", StringType(), True),
    StructField("departure", AIRPORT_SCHEMA, True),
    StructField("arrival", AIRPORT_SCHEMA, True),
    StructField("airline", AIRLINE_SCHEMA, True),
    StructField("flight", FLIGHT_NUMBER_SCHEMA, True),
])

AIRPORT_FIELDS = [
    "airport", "timezone", "iata", "icao", "scheduled", "estimated", "actual",
]
TIME_FIELDS = {"scheduled", "estimated", "actual"}

# Column order and names must match the ClickHouse table.
TARGET_COLUMNS = (
    ["flight_date", "flight_status",
     "flight_number", "flight_iata", "flight_icao",
     "airline_name", "airline_iata", "airline_icao"]
    + [f"dep_{f}" for f in AIRPORT_FIELDS]
    + [f"arr_{f}" for f in AIRPORT_FIELDS]
    + ["kafka_partition", "kafka_offset", "ingested_at"]
)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("clickhouse-flights-loader")
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


# read from kafka
def read_from_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", FLIGHTS_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )


def _airport_columns(prefix: str, struct_name: str) -> list:
    """Flatten a departure/arrival struct into dep_*/arr_* columns."""
    cols = []
    for field in AIRPORT_FIELDS:
        c = col(f"f.{struct_name}.{field}")
        if field in TIME_FIELDS:
            c = to_timestamp(c, TIMESTAMP_FORMAT)
        cols.append(c.alias(f"{prefix}_{field}"))
    return cols


# parse and flatten the rows
def build_rows(kafka_df: DataFrame) -> DataFrame:
    """Turn Kafka messages into flat rows matching the ClickHouse table."""
    parsed = (
        kafka_df
        .selectExpr(
            "CAST(value AS STRING) AS original_json",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
        )
        .withColumn("f", from_json(col("original_json"), FLIGHT_SCHEMA))
    )

    return parsed.select(
        to_date(col("f.flight_date"), DATE_FORMAT).alias("flight_date"),
        col("f.flight_status").alias("flight_status"),
        col("f.flight.number").alias("flight_number"),
        col("f.flight.iata").alias("flight_iata"),
        col("f.flight.icao").alias("flight_icao"),
        col("f.airline.name").alias("airline_name"),
        col("f.airline.iata").alias("airline_iata"),
        col("f.airline.icao").alias("airline_icao"),
        *_airport_columns("dep", "departure"),
        *_airport_columns("arr", "arrival"),
        col("kafka_partition").cast("int").alias("kafka_partition"),
        col("kafka_offset").alias("kafka_offset"),
        current_timestamp().alias("ingested_at"),
    ).select(*TARGET_COLUMNS)


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


def build_redis_df(df: DataFrame) -> DataFrame:
    """Latest state per flight as strings, plus a key and a sort score."""
    return df.select(
        concat_ws(
            ":",
            lit("flight"),
            col("flight_date").cast("string"),
            col("flight_iata"),
            col("dep_scheduled").cast("long").cast("string"),
        ).alias("redis_key"),
        col("dep_scheduled").cast("long").alias("score"),
        *[col(c).cast("string").alias(c) for c in TARGET_COLUMNS],
    )


def write_partition_to_redis(rows) -> None:
    """Runs on executors: upsert each flight as a hash and index it."""
    client = _redis_client()
    pipe = client.pipeline(transaction=False)
    count = 0

    for row in rows:
        data = row.asDict()
        key = data.pop("redis_key")
        score = data.pop("score")
        mapping = {k: v for k, v in data.items() if v is not None}

        pipe.hset(key, mapping=mapping)
        pipe.expire(key, REDIS_TTL_SECONDS)
        pipe.zadd(REDIS_INDEX_KEY, {key: score})
        count += 1

    if count:
        pipe.execute()


def write_to_redis(df: DataFrame, batch_id: int) -> None:
    build_redis_df(df).foreachPartition(write_partition_to_redis)

    client = _redis_client()
    # Drop index entries for flights older than the TTL window.
    client.zremrangebyscore(
        REDIS_INDEX_KEY, "-inf", time.time() - REDIS_TTL_SECONDS
    )
    # Tell the dashboard backend there is fresh data.
    client.publish(REDIS_CHANNEL, str(batch_id))


# write each micro-batch to clickhouse and redis
def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()

    try:
        if batch_df.isEmpty():
            logger.info("Batch %s is empty", batch_id)
            return

        # Rows without a key cannot be stored, since ORDER BY needs
        # flight_date, flight_iata and dep_scheduled.
        good_df = batch_df.filter(
            col("flight_date").isNotNull()
            & col("flight_iata").isNotNull()
            & col("dep_scheduled").isNotNull()
        )
        total = batch_df.count()
        good = good_df.count()
        if good < total:
            logger.warning(
                "Batch %s: dropping %s rows without flight_date, "
                "flight_iata or dep_scheduled",
                batch_id,
                total - good,
            )

        # Dedupe inside this batch. Duplicates across batches (replays,
        # restarts) are collapsed by ReplacingMergeTree.
        deduped_df = good_df.dropDuplicates(
            ["flight_date", "flight_iata", "dep_scheduled"]
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
            "ClickHouse flights loader started (input=%s, target=%s, "
            "startingOffsets=%s, checkpoint=%s)",
            FLIGHTS_TOPIC,
            TARGET_TABLE,
            STARTING_OFFSETS,
            CHECKPOINT_LOCATION,
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()