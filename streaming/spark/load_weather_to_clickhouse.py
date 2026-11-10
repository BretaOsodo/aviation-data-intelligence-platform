import os
import logging

from dotenv import load_dotenv

from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    when,
    concat_ws,
    current_timestamp,
    to_timestamp,
    to_date,
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

CATALOG = "clickhouse"
TARGET_TABLE = f"{CATALOG}.{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"

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

        deduped_df.writeTo(TARGET_TABLE).append()

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