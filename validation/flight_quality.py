
import json
import logging
from pathlib import Path
from datetime import datetime

from fastavro import parse_schema, validate
from fastavro._validate_common import ValidationError

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "schema" / "flights.avsc"


# Load Avro schema once
try:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as file:
        FLIGHTS_SCHEMA = parse_schema(json.load(file))

except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
    raise RuntimeError(
        f"Unable to load flight Avro schema: {exc}"
    ) from exc


def validate_flight_schema(record):
    """Validate a flight record against the Avro schema."""

    if not isinstance(record, dict):
        return ["Flight record must be a dictionary"]

    try:
        validate(record, FLIGHTS_SCHEMA)
        return []

    except (ValidationError, ValueError, TypeError) as exc:
        return [f"Avro schema validation failed: {exc}"]


def validate_flight_business_rules(record):
    """Validate flight-specific business rules."""

    errors = []

    if not isinstance(record, dict):
        return ["Flight record must be a dictionary"]

    # Validate flight date
    flight_date = record.get("flight_date")

    if flight_date:
        try:
            datetime.strptime(flight_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            errors.append("Invalid flight_date format")

    # Validate flight status
    allowed_statuses = {
        "scheduled",
        "active",
        "landed",
        "cancelled",
        "incident",
        "diverted",
        "unknown"
    }

    status = record.get("flight_status")

    if status and status not in allowed_statuses:
        errors.append(f"Invalid flight status: {status}")

    # Validate departure and arrival
    for section in ["departure", "arrival"]:

        details = record.get(section)

        if not isinstance(details, dict):
            continue

        # IATA code
        iata = details.get("iata")

        if iata is not None:
            if (
                not isinstance(iata, str)
                or len(iata) != 3
                or not iata.isalpha()
            ):
                errors.append(f"Invalid {section} IATA code")

        # ICAO code
        icao = details.get("icao")

        if icao is not None:
            if (
                not isinstance(icao, str)
                or len(icao) != 4
                or not icao.isalnum()
            ):
                errors.append(f"Invalid {section} ICAO code")

        # Validate timestamps
        timestamp_fields = [
            "scheduled",
            "estimated",
            "actual",
            "estimated_runway",
            "actual_runway"
        ]

        for field in timestamp_fields:
            timestamp = details.get(field)

            if timestamp is not None:
                try:
                    if not isinstance(timestamp, str):
                        raise TypeError("Timestamp must be a string")

                    parsed = datetime.fromisoformat(
                        timestamp.replace("Z", "+00:00")
                    )

                    if parsed.tzinfo is None:
                        errors.append(
                            f"{section}.{field} must include timezone"
                        )

                except (ValueError, TypeError):
                    errors.append(
                        f"Invalid {section}.{field} timestamp"
                    )

        # Validate delay
        delay = details.get("delay")

        if delay is not None:
            if (
                isinstance(delay, bool)
                or not isinstance(delay, int)
                or delay < 0
            ):
                errors.append(f"Invalid {section} delay")

    return errors


def validate_flight(record):
    """Run schema and business-rule validation."""

    errors = []

    errors.extend(validate_flight_schema(record))
    errors.extend(validate_flight_business_rules(record))

    if errors:
        logger.warning("Flight validation failed: %s", errors)
    else:
        logger.info("Flight record passed validation")

    return errors

