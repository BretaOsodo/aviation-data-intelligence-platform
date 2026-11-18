import os
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    struct,
    lit,
    when,
    concat_ws,
    current_timestamp,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

load_dotenv()


# CONFIGURATION

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:29092,kafka2:29092"
)
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "airport-weather")
VALIDATED_TOPIC = os.getenv("VALIDATED_TOPIC", "airport-validated")

# "earliest" only applies when there is no checkpoint yet.
# After the first run, Spark resumes from the checkpoint.
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

S3_BUCKET = os.getenv("S3_BUCKET")
S3_INVALID_PREFIX = os.getenv("S3_INVALID_PREFIX", "invalid/weather")

# Must be durable: mount a volume at this path, or use an s3a:// path.
# A relative path inside the container is lost when the container is recreated.
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION", "/spark/checkpoint/weather_validation"
)

MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "1000"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

# Identifies this run, so invalid-record files from different runs
# never overwrite each other (batch_id restarts at 0 without a checkpoint).
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("weather-validation")


# WEATHER DATA SCHEMA
# Field names must match the producer's JSON exactly.
# Unknown or misspelled fields do not raise errors in from_json;
# they silently become nulls.

WEATHER_SCHEMA = StructType([
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

    # Populated by Spark for malformed JSON in PERMISSIVE mode.
    StructField("_corrupt_record", StringType(), True),
])

WEATHER_COLUMNS = [
    field.name
    for field in WEATHER_SCHEMA.fields
    if field.name != "_corrupt_record"
]


# SPARK SESSION

def create_spark_session() -> SparkSession:
    access_key = os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "AWS_ACCESS_KEY or AWS_SECRET_KEY is missing from the environment"
        )

    spark = (
        SparkSession.builder
        .appName("JKIA-Weather-Validation")
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


# PARSE KAFKA JSON

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
            "weather",
            from_json(
                col("original_json"),
                WEATHER_SCHEMA,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ),
        )
    )

    return parsed_df.select(
        "original_json",
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        "weather.*",
    )


# VALIDATE WEATHER RECORDS

def validate_events(parsed_df: DataFrame) -> DataFrame:
    df = parsed_df

    df = df.withColumn(
        "observation_timestamp",
        to_timestamp(col("observation_time_utc"), TIMESTAMP_FORMAT),
    )

    df = df.withColumn("weather_code", col("weather_code").cast("int"))

    # Each failed rule contributes a message; concat_ws skips nulls.
    df = df.withColumn(
        "validation_error",
        concat_ws(
            "; ",

            when(
                col("_corrupt_record").isNotNull(),
                lit("Malformed JSON record"),
            ),

            when(
                col("airport_icao").isNull() |
                (col("airport_icao") != "HKJK"),
                lit("Invalid or missing airport ICAO; expected HKJK"),
            ),

            when(
                col("airport_iata").isNull() |
                (col("airport_iata") != "NBO"),
                lit("Invalid or missing airport IATA; expected NBO"),
            ),

            when(
                col("airport_name").isNull() |
                (col("airport_name") == ""),
                lit("Airport name is missing"),
            ),

            when(
                col("latitude").isNull() |
                (col("latitude") < -90) |
                (col("latitude") > 90),
                lit("Latitude is missing or outside valid bounds"),
            ),

            when(
                col("longitude").isNull() |
                (col("longitude") < -180) |
                (col("longitude") > 180),
                lit("Longitude is missing or outside valid bounds"),
            ),

            when(
                col("observation_time_utc").isNull() |
                col("observation_timestamp").isNull(),
                lit("Observation timestamp is missing or invalid"),
            ),

            # Required measurements. Without this rule, a schema mismatch
            # would pass every record as "valid" with all values null.
            when(
                col("temperature_c").isNull() |
                col("surface_pressure_hpa").isNull() |
                col("wind_speed_knots").isNull(),
                lit("Required measurement is missing"),
            ),

            when(
                col("relative_humidity_pct").isNotNull() &
                (
                    (col("relative_humidity_pct") < 0) |
                    (col("relative_humidity_pct") > 100)
                ),
                lit("Relative humidity must be between 0 and 100"),
            ),

            when(
                col("cloud_cover_pct").isNotNull() &
                (
                    (col("cloud_cover_pct") < 0) |
                    (col("cloud_cover_pct") > 100)
                ),
                lit("Cloud cover must be between 0 and 100"),
            ),

            when(
                col("wind_direction_degrees").isNotNull() &
                (
                    (col("wind_direction_degrees") < 0) |
                    (col("wind_direction_degrees") > 360)
                ),
                lit("Wind direction must be between 0 and 360"),
            ),

            when(
                (col("precipitation_mm").isNotNull() &
                 (col("precipitation_mm") < 0)) |
                (col("rain_mm").isNotNull() &
                 (col("rain_mm") < 0)),
                lit("Precipitation or rain cannot be negative"),
            ),

            when(
                (col("wind_speed_knots").isNotNull() &
                 (col("wind_speed_knots") < 0)) |
                (col("wind_gusts_knots").isNotNull() &
                 (col("wind_gusts_knots") < 0)),
                lit("Wind speed or gusts cannot be negative"),
            ),

            when(
                col("visibility_m").isNotNull() &
                (col("visibility_m") < 0),
                lit("Visibility cannot be negative"),
            ),
        ),
    )

    df = df.withColumn("is_valid", col("validation_error") == "")

    return df


# WRITE VALID RECORDS TO KAFKA

def write_valid_records(valid_df: DataFrame) -> None:
    # Drop duplicates inside the batch. The key lets downstream consumers
    # (or log compaction) deduplicate across batches and retries, because
    # Kafka writes from foreachBatch are at-least-once.
    deduped_df = valid_df.dropDuplicates(
        ["airport_icao", "observation_time_utc"]
    )

    kafka_output = deduped_df.select(
        concat_ws(
            "|", col("airport_icao"), col("observation_time_utc")
        ).alias("key"),
        to_json(
            struct(*[col(name) for name in WEATHER_COLUMNS])
        ).alias("value"),
    )

    (
        kafka_output.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", VALIDATED_TOPIC)
        .save()
    )


# WRITE INVALID RECORDS TO S3

def write_invalid_records(invalid_df: DataFrame, batch_id: int) -> None:
    s3_path = (
        f"s3a://{S3_BUCKET}/{S3_INVALID_PREFIX}/"
        f"run_id={RUN_ID}/batch_id={batch_id}"
    )

    output_df = (
        invalid_df
        .select(
            "original_json",
            "airport_icao",
            "airport_iata",
            "airport_name",
            "city",
            "observation_time_utc",
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

    logger.info("Wrote invalid weather records to %s", s3_path)


# SPLIT AND WRITE EACH MICRO-BATCH

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    # The batch is used several times below; cache it so Spark does not
    # re-read Kafka and re-run the validation for each action.
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
            "JKIA weather validation pipeline started "
            "(run_id=%s, startingOffsets=%s, checkpoint=%s)",
            RUN_ID,
            STARTING_OFFSETS,
            CHECKPOINT_LOCATION,
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()