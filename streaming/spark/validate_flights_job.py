import os
import logging
from datetime import datetime , timezone

from dotenv import load_dotenv

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

#configurations
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29092"
INPUT_TOPIC= "airport-flights"
VALIDATED_TOPIC="aiport-flights-validated"

#earliest only applies when no checkpoint sexists yet
STARTING_OFFSETS= "earliest"

S3_BUCKET= os.getenv("AIRPORT_INVALID_DATA")
S3_INVALID_PREFIX= os.getenv("AIRPORT_INVALID_PREFIX")

#Must be durable
CHECKPOINT_LOCATION="/spark/checkpoint/flight_validation"

MAX_OFFSETS_PER_TRIGGER= 1000
TRIGGER_INTERVAL="30 seconds"

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"
DATE_FORMAT = "yyyy-MM-dd"

ALLOWED_FLIGHT_STATUSES = [
    "scheduled",
    "active",
    "landed",
    "cancelled",
    "incident",
    "diverted",
]

IATA_AIRPORT_REGEX = r"^[A-Z]{3}$"
ICAO_AIRPORT_REGEX = r"^[A-Z]{4}$"
AIRLINE_IATA_REGEX = r"^[A-Z0-9]{2}$"
FLIGHT_IATA_REGEX = r"^[A-Z0-9]{2}[0-9]{1,4}[A-Z]?$"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flight-validation")

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

#SPARK SESSION
def create_spark_session() -> SparkSession:
    access_key = os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "AWS_ACCESS_KEY or AWS_SECRET_KEY is missing from the environment"
        )
    spark = (
        SparkSession.builder
        .appName("Flight-Schedule-Validation")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        .config("spark.hadoop.fs.s3a.max.total.tasks", "5")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        # Bad dates/timestamps must become NULL (and be reported as
        # validation errors), not raise an exception that kills the stream.
        # This is the default in Spark 3.x; Spark 4 turns ANSI mode on.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

# READ FROM KAFKA
def read_from_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

#PARSE KAFKA JSON
def parse_events(kafka_df: DataFrame) -> DataFrame:
    parsed_df = (
        kafka_df
        .selectExpr(
            "CAST(value AS STRING) AS original_json",
            "timestamp AS kafka_timestamp",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
        )
        .withColumn(
            "f",
            from_json(
                col("original_json"),
                FLIGHT_SCHEMA,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ),
        )
    )

    # Flatten the nested structure into plain columns.
    return parsed_df.select(
        "original_json",
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        col("f._corrupt_record").alias("_corrupt_record"),
        col("f.flight_date").alias("flight_date"),
        col("f.flight_status").alias("flight_status"),
        col("f.departure.airport").alias("dep_airport"),
        col("f.departure.timezone").alias("dep_timezone"),
        col("f.departure.iata").alias("dep_iata"),
        col("f.departure.icao").alias("dep_icao"),
        col("f.departure.scheduled").alias("dep_scheduled"),
        col("f.departure.estimated").alias("dep_estimated"),
        col("f.departure.actual").alias("dep_actual"),
        col("f.arrival.airport").alias("arr_airport"),
        col("f.arrival.timezone").alias("arr_timezone"),
        col("f.arrival.iata").alias("arr_iata"),
        col("f.arrival.icao").alias("arr_icao"),
        col("f.arrival.scheduled").alias("arr_scheduled"),
        col("f.arrival.estimated").alias("arr_estimated"),
        col("f.arrival.actual").alias("arr_actual"),
        col("f.airline.name").alias("airline_name"),
        col("f.airline.iata").alias("airline_iata"),
        col("f.airline.icao").alias("airline_icao"),
        col("f.flight.number").alias("flight_number"),
        col("f.flight.iata").alias("flight_iata"),
        col("f.flight.icao").alias("flight_icao"),
    )

#Validation Helpers
def is_missing(column):
    """True when a value is null or only whitespace."""
    return column.isNull() | (trim(column) == "")


def bad_optional_timestamp(raw, parsed):
    """True when an optional timestamp is present but cannot be parsed."""
    return (~is_missing(raw)) & parsed.isNull()

#Validate Flight Records

def validate_events(parsed_df: DataFrame) -> DataFrame:
    df = parsed_df

    df = (
        df
        .withColumn("flight_date_parsed",
                    to_date(col("flight_date"), DATE_FORMAT))
        .withColumn("dep_scheduled_ts",
                    to_timestamp(col("dep_scheduled"), TIMESTAMP_FORMAT))
        .withColumn("dep_estimated_ts",
                    to_timestamp(col("dep_estimated"), TIMESTAMP_FORMAT))
        .withColumn("dep_actual_ts",
                    to_timestamp(col("dep_actual"), TIMESTAMP_FORMAT))
        .withColumn("arr_scheduled_ts",
                    to_timestamp(col("arr_scheduled"), TIMESTAMP_FORMAT))
        .withColumn("arr_estimated_ts",
                    to_timestamp(col("arr_estimated"), TIMESTAMP_FORMAT))
        .withColumn("arr_actual_ts",
                    to_timestamp(col("arr_actual"), TIMESTAMP_FORMAT))
    )

    # Each failed rule contributes one message; concat_ws skips nulls.
    # A rule whose condition evaluates to null simply does not fire,
    # so every required field has its own explicit "missing" rule.
    df = df.withColumn(
        "validation_error",
        concat_ws(
            "; ",

            when(
                col("_corrupt_record").isNotNull(),
                lit("Malformed JSON record"),
            ),

            # Flight date and status
            when(
                is_missing(col("flight_date")) |
                col("flight_date_parsed").isNull(),
                lit("flight_date is missing or not a valid yyyy-MM-dd date"),
            ),

            when(
                is_missing(col("flight_status")) |
                (~col("flight_status").isin(*ALLOWED_FLIGHT_STATUSES)),
                lit("flight_status is missing or not an allowed value"),
            ),

            # Flight identity
            when(
                is_missing(col("flight_number")),
                lit("flight.number is missing"),
            ),

            when(
                is_missing(col("flight_iata")) |
                (~col("flight_iata").rlike(FLIGHT_IATA_REGEX)),
                lit("flight.iata is missing or malformed"),
            ),

            when(
                is_missing(col("airline_iata")) |
                (~col("airline_iata").rlike(AIRLINE_IATA_REGEX)),
                lit("airline.iata is missing or malformed"),
            ),

            # Departure airport
            when(
                is_missing(col("dep_iata")) |
                (~col("dep_iata").rlike(IATA_AIRPORT_REGEX)),
                lit("departure.iata is missing or not a 3-letter code"),
            ),

            when(
                (~is_missing(col("dep_icao"))) &
                (~col("dep_icao").rlike(ICAO_AIRPORT_REGEX)),
                lit("departure.icao is not a 4-letter code"),
            ),

            # Arrival airport
            when(
                is_missing(col("arr_iata")) |
                (~col("arr_iata").rlike(IATA_AIRPORT_REGEX)),
                lit("arrival.iata is missing or not a 3-letter code"),
            ),

            when(
                (~is_missing(col("arr_icao"))) &
                (~col("arr_icao").rlike(ICAO_AIRPORT_REGEX)),
                lit("arrival.icao is not a 4-letter code"),
            ),

            when(
                col("dep_iata").isNotNull() &
                (col("dep_iata") == col("arr_iata")),
                lit("Departure and arrival airports are the same"),
            ),

            # Scheduled times are required
            when(
                is_missing(col("dep_scheduled")) |
                col("dep_scheduled_ts").isNull(),
                lit("departure.scheduled is missing or not an ISO-8601 "
                    "timestamp with offset"),
            ),

            when(
                is_missing(col("arr_scheduled")) |
                col("arr_scheduled_ts").isNull(),
                lit("arrival.scheduled is missing or not an ISO-8601 "
                    "timestamp with offset"),
            ),

            # Optional times must parse when present
            when(
                bad_optional_timestamp(
                    col("dep_estimated"), col("dep_estimated_ts")
                ) |
                bad_optional_timestamp(
                    col("dep_actual"), col("dep_actual_ts")
                ) |
                bad_optional_timestamp(
                    col("arr_estimated"), col("arr_estimated_ts")
                ) |
                bad_optional_timestamp(
                    col("arr_actual"), col("arr_actual_ts")
                ),
                lit("An estimated or actual timestamp is not a valid "
                    "ISO-8601 timestamp"),
            ),

            # Business rule: a flight cannot arrive before it departs
            when(
                col("dep_scheduled_ts").isNotNull() &
                col("arr_scheduled_ts").isNotNull() &
                (col("arr_scheduled_ts") <= col("dep_scheduled_ts")),
                lit("arrival.scheduled is not after departure.scheduled"),
            ),
        ),
    )

    df = df.withColumn("is_valid", col("validation_error") == "")

    return df

#Write valid records to kafka
def write_valid_records(valid_df: DataFrame) -> None:
    # Same flight, same scheduled departure = same record. This removes
    # duplicates inside a batch. Kafka writes from foreachBatch are
    # at-least-once, so consumers must still deduplicate on the key.
    deduped_df = valid_df.dropDuplicates(
        ["flight_date", "flight_iata", "dep_iata", "dep_scheduled"]
    )

    kafka_output = deduped_df.select(
        concat_ws(
            "|",
            col("flight_date"),
            col("flight_iata"),
            col("dep_iata"),
            col("dep_scheduled"),
        ).alias("key"),
        # Forward the original message untouched, so fields that are not
        # validated (aircraft, live, delay, gate...) are not lost.
        col("original_json").alias("value"),
    )

    (
        kafka_output.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", VALIDATED_TOPIC)
        .save()
    )

#Write Invalid records to s3
def write_invalid_records(invalid_df: DataFrame, batch_id: int) -> None:
    s3_path = (
        f"s3a://{S3_BUCKET}/{S3_INVALID_PREFIX}/"
        f"run_id={RUN_ID}/batch_id={batch_id}"
    )

    output_df = (
        invalid_df
        .select(
            "original_json",
            "flight_date",
            "flight_iata",
            "dep_iata",
            "arr_iata",
            "dep_scheduled",
            "arr_scheduled",
            "validation_error",
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
        )
        .withColumn("processed_at", current_timestamp())
        .withColumn("run_id", lit(RUN_ID))
        .withColumn("batch_id", lit(batch_id))
    )

    # Append, so a retried batch adds files instead of deleting earlier ones.
    output_df.write.mode("append").parquet(s3_path)

    logger.info("Wrote invalid flight records to %s", s3_path)


# SPLIT AND WRITE EACH MICRO-BATCH

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    # Used several times below; cache so Spark does not re-read Kafka
    # and re-run the validation for every action.
    batch_df.persist()

    try:
        if batch_df.isEmpty():
            logger.info("Batch %s is empty", batch_id)
            return

        valid_df = batch_df.filter(col("is_valid"))
        invalid_df = batch_df.filter(~col("is_valid"))

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        logger.info(
            "Batch %s: %s valid records, %s invalid records",
            batch_id,
            valid_count,
            invalid_count,
        )

        if valid_count > 0:
            write_valid_records(valid_df)

        if invalid_count > 0:
            write_invalid_records(invalid_df, batch_id)

    finally:
        batch_df.unpersist()


# MAIN STREAMING PIPELINE

def main():
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable is not configured")

    spark = create_spark_session()

    try:
        kafka_df = read_from_kafka(spark)
        parsed_df = parse_events(kafka_df)
        validated_df = validate_events(parsed_df)

        query = (
            validated_df.writeStream
            .outputMode("append")
            .foreachBatch(process_batch)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .start()
        )

        logger.info(
            "Flight validation pipeline started "
            "(run_id=%s, input=%s, output=%s, startingOffsets=%s, "
            "checkpoint=%s)",
            RUN_ID,
            INPUT_TOPIC,
            VALIDATED_TOPIC,
            STARTING_OFFSETS,
            CHECKPOINT_LOCATION,
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()