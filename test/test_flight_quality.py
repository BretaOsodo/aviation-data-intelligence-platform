
import copy

import pytest

from validation.flight_quality import (
    validate_flight_schema,
    validate_flight_business_rules,
    validate_flight
)


@pytest.fixture
def valid_flight():
    return {
        "flight_date": "2026-10-30",
        "flight_status": "scheduled",
        "departure": {
            "airport": "Jomo Kenyatta International",
            "timezone": "Africa/Nairobi",
            "iata": "NBO",
            "icao": "HKJK",
            "terminal": "2",
            "gate": None,
            "delay": None,
            "scheduled": "2026-10-30T11:00:00+00:00",
            "estimated": "2026-10-30T11:00:00+00:00",
            "actual": None,
            "estimated_runway": None,
            "actual_runway": None
        },
        "arrival": {
            "airport": "Ukunda",
            "timezone": "Africa/Nairobi",
            "iata": "UKA",
            "icao": "HKUK",
            "terminal": None,
            "gate": None,
            "baggage": None,
            "delay": None,
            "scheduled": "2026-10-30T12:10:00+00:00",
            "estimated": None,
            "actual": None,
            "estimated_runway": None,
            "actual_runway": None
        },
        "airline": {
            "name": "empty",
            "iata": None,
            "icao": None
        },
        "flight": {
            "number": "2641",
            "iata": None,
            "icao": None,
            "codeshared": None
        },
        "aircraft": None,
        "live": None
    }


def test_valid_flight_schema(valid_flight):
    assert validate_flight_schema(valid_flight) == []


def test_invalid_flight_schema(valid_flight):
    invalid = copy.deepcopy(valid_flight)

    invalid["departure"]["delay"] = "late"

    errors = validate_flight_schema(invalid)

    assert len(errors) > 0


def test_invalid_iata_code(valid_flight):
    invalid = copy.deepcopy(valid_flight)

    invalid["departure"]["iata"] = "NAIROBI"

    errors = validate_flight_business_rules(invalid)

    assert any("IATA" in error for error in errors)


def test_invalid_timestamp(valid_flight):
    invalid = copy.deepcopy(valid_flight)

    invalid["departure"]["scheduled"] = "not-a-timestamp"

    errors = validate_flight_business_rules(invalid)

    assert any("timestamp" in error for error in errors)


def test_invalid_flight_status(valid_flight):
    invalid = copy.deepcopy(valid_flight)

    invalid["flight_status"] = "flying_to_mars"

    errors = validate_flight_business_rules(invalid)

    assert any("flight status" in error for error in errors)


def test_negative_delay(valid_flight):
    invalid = copy.deepcopy(valid_flight)

    invalid["departure"]["delay"] = -20

    errors = validate_flight_business_rules(invalid)

    assert any("delay" in error for error in errors)


def test_complete_flight_validation(valid_flight):
    assert validate_flight(valid_flight) == []
