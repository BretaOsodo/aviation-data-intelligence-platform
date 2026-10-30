
import json
import os
import time
import logging

from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

from validation.weather_quality import validate_weather
from streaming.api.weather_api import get_weather_data



# 1. PROJECT CONFIGURATION


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)



# 2. KAFKA CONFIGURATION


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "127.0.0.1:9092,127.0.0.1:9093",
)

WEATHER_TOPIC = "airport-weather"
DLQ_TOPIC = "airport-weather-dlq"

POLL_INTERVAL_SECONDS = int(
    os.getenv("WEATHER_POLL_INTERVAL_SECONDS", "300")
)

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "weather-events-producer",
    "acks": "all",
    "retries": 5,
    "enable.idempotence": True,
}

producer = Producer(producer_config)

string_serializer = StringSerializer()



# 3. KAFKA DELIVERY CALLBACK


def delivery_report(err, msg):

    if err:

        logger.error(
            "Failed to deliver message to %s: %s",
            msg.topic(),
            err,
        )

    else:

        logger.info(
            "Message delivered | Topic: %s | Partition: %s | Offset: %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )



# 4. SAFE KAFKA PUBLISHING


def produce_message(**kwargs):
    """
    Publish a message to Kafka.

    If the local producer queue is full,
    poll Kafka until space becomes available.
    """

    while True:

        try:

            producer.produce(
                **kwargs
            )

            producer.poll(0)

            return

        except BufferError:

            logger.warning(
                "Kafka producer queue is full. Waiting for delivery..."
            )

            producer.poll(1)



# 5. PUBLISH VALID WEATHER RECORDS


def publish_valid_weather(record):
    """
    Serialize valid weather records into JSON
    and publish them to the main Kafka topic.
    """

    serialized_value = json.dumps(
        record,
        allow_nan=False,
    )

    produce_message(
        topic=WEATHER_TOPIC,
        key=string_serializer(record["airport_icao"]),
        value=serialized_value,
        callback=delivery_report,
    )



# 6. PUBLISH INVALID RECORDS TO DEAD LETTER QUEUE


def publish_invalid_weather_dlq(record, errors):
    """
    Preserve failed records and their error details
    in the Kafka Dead Letter Queue.
    """

    if isinstance(record, dict):

        airport_code = record.get(
            "airport_icao",
            "unknown",
        )

    else:

        airport_code = "unknown"

    dead_letter_record = {
        "source": "weather_api",

        "record": record,

        "validation_errors": errors,

        "failed_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    serialized_value = json.dumps(
        dead_letter_record,
        default=str,
        allow_nan=False,
    )

    produce_message(
        topic=DLQ_TOPIC,
        key=string_serializer(str(airport_code)),
        value=serialized_value,
        callback=delivery_report,
    )

    logger.warning(
        "Record sent to DLQ | Airport: %s | Errors: %s",
        airport_code,
        errors,
    )



# 7. VALIDATE AND ROUTE WEATHER RECORDS


def process_weather_record(record):
    """
    Validate one weather record.

    Valid records go to airport-weather.
    Invalid records go to airport-weather-dlq.
    """

    # Check that the record is a dictionary.

    if not isinstance(record, dict):

        errors = [
            f"Expected a dictionary but received "
            f"{type(record).__name__}"
        ]

        logger.error(errors[0])

        try:

            publish_invalid_weather_dlq(
                record,
                errors,
            )

        except Exception:

            logger.exception(
                "Failed to publish malformed record to DLQ"
            )

        return False

    try:

        # Run weather quality validation.

        errors = validate_weather(record)

        # Route invalid records to DLQ.

        if errors:

            logger.warning(
                "Weather validation failed for %s: %s",
                record.get("airport_icao", "unknown"),
                errors,
            )

            publish_invalid_weather_dlq(
                record,
                errors,
            )

            return False

        # Publish valid records.

        publish_valid_weather(record)

        logger.info(
            "Valid weather record queued for %s",
            record["airport_icao"],
        )

        return True

    except Exception as exc:

        logger.exception(
            "Error processing weather record for %s",
            record.get("airport_icao", "unknown"),
        )

        try:

            publish_invalid_weather_dlq(
                record,
                [f"Processing error: {exc}"],
            )

        except Exception:

            logger.exception(
                "Could not publish failed record to DLQ"
            )

        return False



# 8. NORMALIZE WEATHER API RESPONSE


def normalize_weather_data(data):
    """
    Ensure the API response is a list of dictionaries.
    """

    # Decode JSON text if returned by the API.

    if isinstance(data, str):

        data = json.loads(data)

    # Convert a single dictionary into a list.

    if isinstance(data, dict):

        data = [data]

    # Reject unexpected response types.

    if not isinstance(data, list):

        raise TypeError(
            "Weather API must return a list of dictionaries"
        )

    return data



# 9. MAIN WEATHER STREAMING PIPELINE


def run_weather_streaming():

    logger.info(
        "Starting aviation weather streaming"
    )

    logger.info(
        "Kafka bootstrap servers: %s",
        KAFKA_BOOTSTRAP_SERVERS,
    )

    logger.info(
        "Weather polling interval: %s seconds",
        POLL_INTERVAL_SECONDS,
    )

    try:

        while True:

            valid_count = 0
            invalid_count = 0

            try:

                # Fetch weather records from Open-Meteo.

                weather_data = get_weather_data()

                # Normalize API response.

                weather_records = normalize_weather_data(
                    weather_data
                )

                logger.info(
                    "Received %s weather records from API",
                    len(weather_records),
                )

                # Process each weather record.

                for record in weather_records:

                    if process_weather_record(record):

                        valid_count += 1

                    else:

                        invalid_count += 1

            except Exception:

                logger.exception(
                    "Weather ingestion cycle failed"
                )

            # Wait for Kafka messages to be delivered.

            remaining = producer.flush(
                timeout=30
            )

            if remaining:

                logger.error(
                    "%s Kafka messages remain undelivered",
                    remaining,
                )

            # Log ingestion statistics.

            logger.info(
                "Weather ingestion cycle completed | "
                "Valid: %s | Invalid: %s",
                valid_count,
                invalid_count,
            )

            # Wait before the next API request.

            logger.info(
                "Next weather ingestion cycle in %s seconds",
                POLL_INTERVAL_SECONDS,
            )

            time.sleep(
                POLL_INTERVAL_SECONDS
            )

    except KeyboardInterrupt:

        logger.info(
            "Weather streaming interrupted by user"
        )

    finally:

        logger.info(
            "Flushing remaining Kafka messages..."
        )

        remaining = producer.flush(
            timeout=30
        )

        if remaining:

            logger.error(
                "%s messages could not be delivered",
                remaining,
            )

        logger.info(
            "Weather producer stopped"
        )



# 10. APPLICATION ENTRY POINT


if __name__ == "__main__":

    run_weather_streaming()
