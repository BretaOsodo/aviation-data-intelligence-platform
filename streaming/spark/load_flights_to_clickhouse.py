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
    trim
)
from pyspark.sql.types import *

from streaming.spark.load_weather_to_clickhouse import KAFKA_BOOTSTRAP_SERVERS
from streaming.spark.validate_weather_job import STARTING_OFFSETS

load_dotenv()

#configurations
KAFKA_BOOTSTRAP_SERVERS="kafka1:29092, kafka2;29092"
FLIGHTS_TOPIC=

STARTING_OFFSETS="earliest"
CLICKHOUSE_HOST="clickhouse"
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER= os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "aviation_streaming")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "airport_flights")

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

    # Populated by Spark for malformed JSON in PERMISSIVE mode.
    StructField("_corrupt_record", StringType(), True),
])

TARGET_COLUMNS = [
    "airport",
    "timezone",
    "iata",
    "icao",
    "scheduled",
    "estimated",
    "actual",
    "name",
    "number",
    "flight_date",
    "flight_status",
    "departure",
    "arrival",
    "airline",
    "flight",
    "_corrupt_record",
]

def create_spark_session() -> SparkSession:
    spark = (
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

    spark.sparkContext.setLogLevel("ERROR")
    return spark

#read from spark
