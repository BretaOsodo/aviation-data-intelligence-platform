
import json
import os
import time
import logging
import math

from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

from validation.flight_quality import validate_flight
from streaming.api.flights import get_airports



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
    "kafka1:29092,kafka2:29092",
)

FLIGHTS_TOPIC = "airport-flights"
DLQ_TOPIC = "airport-flights-dlq"

POLL_INTERVAL_SECONDS = int(
    os.getenv("FLIGHTS_POLL_INTERVAL_SECONDS", "300")
)

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "flights-events-producer",
    "acks": "all",
    "retries": 5,
    "enable.idempotence": True,
}

producer = Producer(producer_config)



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



# 4. FLIGHT MESSAGE KEY


def get_flight_key(record):
    """
    Generate a Kafka message key for each flight.

    Uses flight ICAO, IATA or number, together with
    flight date and departure airport where available.
    """

    if not isinstance(record, dict):
        return "unknown"

    flight = record.get("flight") or {}
    departure = record.get("departure") or {}

    flight_identifier = (
        flight.get("icao")
        or flight.get("iata")
        or flight.get("number")
    )

    departure_airport = (
        departure.get("icao")
        or departure.get("iata")
        or departure.get("airport")
        or "unknown"
    )

    flight_date = record.get("flight_date", "unknown")

    if flight_identifier:

        return (
            f"{flight_date}-"
            f"{departure_airport}-"
            f"{flight_identifier}"
        )

    return (
        f"{flight_date}-"
        f"{departure_airport}-unknown"
    )



# 5. SAFE KAFKA PUBLISHING


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

            return True

        except BufferError:

            logger.warning(
                "Kafka producer queue is full. Waiting for delivery..."
            )

            producer.poll(1)



# 6. JSON SERIALIZATION


def make_json_safe(value):
    """
    Convert non-standard JSON values into JSON-compatible values.
    """

    if isinstance(value, float) and not math.isfinite(value):
        return None

    if isinstance(value, dict):

        return {
            str(key): make_json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):

        return [
            make_json_safe(item)
            for item in value
        ]

    return value


def serialize_record(record):

    return json.dumps(
        make_json_safe(record),
        allow_nan=False,
        ensure_ascii=False,
    ).encode("utf-8")



# 7. PUBLISH VALID FLIGHT RECORDS


def publish_valid_flights(record):
    """
    Serialize valid flight records into JSON
    and publish them to the main Kafka topic.
    """

    try:

        serialized_value = serialize_record(record)

        flight_key = get_flight_key(record)

        produce_message(
            topic=FLIGHTS_TOPIC,
            key=flight_key.encode("utf-8"),
            value=serialized_value,
            on_delivery=delivery_report,
        )

        return True

    except (TypeError, ValueError):

        logger.exception(
            "Could not serialize valid flight record"
        )

        return False



# 8. PUBLISH INVALID RECORDS TO DEAD LETTER QUEUE


def publish_invalid_flights_dlq(record, errors):
    """
    Preserve failed flight records and their validation errors
    in the Kafka Dead Letter Queue.
    """

    flight_key = get_flight_key(record)

    dead_letter_record = {
        "source": "flights_api",
        "source_topic": FLIGHTS_TOPIC,
        "record": record,
        "validation_errors": errors,
        "error_type": "FLIGHT_VALIDATION_ERROR",
        "failed_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    try:

        serialized_value = serialize_record(
            dead_letter_record
        )

        produce_message(
            topic=DLQ_TOPIC,
            key=flight_key.encode("utf-8"),
            value=serialized_value,
            on_delivery=delivery_report,
        )

        logger.warning(
            "Record sent to DLQ | Flight: %s | Errors: %s",
            flight_key,
            errors,
        )

        return True

    except (TypeError, ValueError):

        logger.exception(
            "Could not serialize flight DLQ record"
        )

        return False



# 9. VALIDATE AND ROUTE FLIGHT RECORDS


def process_flight_record(record):
    """
    Validate one flight record.

    Valid records go to airport-flights.
    Invalid records go to airport-flights-dlq.
    """

    # Check that the record is a dictionary.

    if not isinstance(record, dict):

        errors = [
            f"Expected a dictionary but received "
            f"{type(record).__name__}"
        ]

        logger.error(errors[0])

        publish_invalid_flights_dlq(
            record,
            errors,
        )

        return False

    try:

        # Run flight quality validation.

        errors = validate_flight(record)

        # Route invalid records to DLQ.

        if errors:

            logger.warning(
                "Flight validation failed | Flight: %s | Errors: %s",
                get_flight_key(record),
                errors,
            )

            publish_invalid_flights_dlq(
                record,
                errors,
            )

            return False

        # Publish valid flight records.

        published = publish_valid_flights(record)

        if published:

            logger.info(
                "Valid flight record queued | Flight: %s",
                get_flight_key(record),
            )

            return True

        return False

    except Exception as exc:

        logger.exception(
            "Error processing flight record | Flight: %s",
            get_flight_key(record),
        )

        publish_invalid_flights_dlq(
            record,
            [f"Processing error: {exc}"],
        )

        return False



# 10. NORMALIZE FLIGHT API RESPONSE


def normalize_flight_data(data):
    """
    Ensure the API response is a list of dictionaries.
    """

    # Decode JSON text if returned by the API.

    if isinstance(data, str):

        data = json.loads(data)

    # Convert a single dictionary into a list.

    if isinstance(data, dict):

        # Some flight APIs wrap records in a data field.
        if isinstance(data.get("data"), list):

            data = data["data"]

        else:

            data = [data]

    # Reject unexpected response types.

    if not isinstance(data, list):

        raise TypeError(
            "Flight API must return a list of dictionaries"
        )

    return data



# 11. MAIN FLIGHT STREAMING PIPELINE


def run_flights_streaming():

    logger.info(
        "Starting aviation flight streaming"
    )

    logger.info(
        "Kafka bootstrap servers: %s",
        KAFKA_BOOTSTRAP_SERVERS,
    )

    logger.info(
        "Flight polling interval: %s seconds",
        POLL_INTERVAL_SECONDS,
    )

    try:

        while True:

            valid_count = 0
            invalid_count = 0

            try:

                # Fetch flight records from the API.

                flight_data = get_airports()

                # Normalize API response.

                flight_records = normalize_flight_data(
                    flight_data
                )

                logger.info(
                    "Received %s flight records from API",
                    len(flight_records),
                )

                # Process each flight record.

                for record in flight_records:

                    if process_flight_record(record):

                        valid_count += 1

                    else:

                        invalid_count += 1

            except Exception:

                logger.exception(
                    "Flight ingestion cycle failed"
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
                "Flight ingestion cycle completed | "
                "Valid: %s | Invalid: %s",
                valid_count,
                invalid_count,
            )

            # Wait before the next API request.

            logger.info(
                "Next flight ingestion cycle in %s seconds",
                POLL_INTERVAL_SECONDS,
            )

            time.sleep(
                POLL_INTERVAL_SECONDS
            )

    except KeyboardInterrupt:

        logger.info(
            "Flight streaming interrupted by user"
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
            "Flight producer stopped"
        )



# 12. APPLICATION ENTRY POINT


if __name__ == "__main__":

    run_flights_streaming()

