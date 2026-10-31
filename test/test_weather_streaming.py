
import json
import pytest

from streaming.kafka import weather_streaming


# Mock Kafka producer: prevents tests from connecting to Kafka
class MockProducer:

    def __init__(self):
        self.messages = []

    def produce(self, topic, key=None, value=None, callback=None):
        message = {
            "topic": topic,
            "key": key,
            "value": value,
            "callback": callback
        }

        self.messages.append(message)

    def poll(self, timeout=0):
        pass

    def flush(self, timeout=None):
        return 0


@pytest.fixture
def mock_kafka(monkeypatch):
    producer = MockProducer()

    monkeypatch.setattr(
        weather_streaming,
        "producer",
        producer
    )

    # Replace the serializer with a simple test serializer
    monkeypatch.setattr(
        weather_streaming,
        "string_serializer",
        lambda value: value
    )

    return producer


@pytest.fixture
def valid_weather_record():
    return {
        "airport_icao": "HKJK",
        "airport_iata": "NBO",
        "airport_name": "Jomo Kenyatta International Airport",
        "city": "Nairobi",
        "latitude": -1.3192,
        "longitude": 36.9278,
        "elevation_m": 1624,
        "observation_time_utc": "2026-10-30T10:00:00+00:00",
        "temperature_c": 24.5,
        "relative_humidity_pct": 65,
        "apparent_temperature_c": 25.0,
        "precipitation_mm": 0.0,
        "rain_mm": 0.0,
        "weather_code": 1,
        "cloud_cover_pct": 20,
        "wind_speed_knots": 8.5,
        "wind_direction_degrees": 180,
        "wind_gusts_knots": 12.0,
        "surface_pressure_hpa": 1013.2,
        "visibility_m": 10000
    }


def test_valid_weather_record_published(
    mock_kafka,
    valid_weather_record,
    monkeypatch
):
    # Simulate successful validation
    monkeypatch.setattr(
        weather_streaming,
        "validate_weather",
        lambda record: []
    )

    weather_streaming.process_weather_record(
        valid_weather_record
    )

    assert len(mock_kafka.messages) == 1

    message = mock_kafka.messages[0]

    assert message["topic"] == weather_streaming.WEATHER_TOPIC
    assert message["key"] == "HKJK"

    decoded = json.loads(message["value"])

    assert decoded["airport_icao"] == "HKJK"
    assert decoded["temperature_c"] == 24.5


def test_invalid_weather_record_sent_to_dlq(
    mock_kafka,
    valid_weather_record,
    monkeypatch
):
    errors = ["Temperature is outside the accepted range"]

    monkeypatch.setattr(
        weather_streaming,
        "validate_weather",
        lambda record: errors
    )

    weather_streaming.process_weather_record(
        valid_weather_record
    )

    assert len(mock_kafka.messages) == 1

    message = mock_kafka.messages[0]

    assert message["topic"] == weather_streaming.DLQ_TOPIC

    decoded = json.loads(message["value"])

    assert decoded["source"] == "weather_streaming"
    assert decoded["validation_errors"] == errors


def test_json_serialization():
    record = {
        "airport_icao": "HKJK",
        "temperature_c": 24.5
    }

    serialized = json.dumps(
        record,
        allow_nan=False
    )

    decoded = json.loads(serialized)

    assert decoded == record


def test_invalid_json_nan_rejected():
    record = {
        "airport_icao": "HKJK",
        "temperature_c": float("nan")
    }

    with pytest.raises(ValueError):
        json.dumps(record, allow_nan=False)


def test_non_dictionary_record_rejected(
    mock_kafka
):
    weather_streaming.process_weather_record(
        ["invalid", "weather", "record"]
    )

    assert len(mock_kafka.messages) == 1

    message = mock_kafka.messages[0]

    assert message["topic"] == weather_streaming.DLQ_TOPIC


def test_normalize_weather_data_list(
    valid_weather_record
):
    result = weather_streaming.normalize_weather_data(
        [valid_weather_record]
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["airport_icao"] == "HKJK"


def test_normalize_weather_data_invalid_input():

    with pytest.raises(ValueError):
        weather_streaming.normalize_weather_data(12345)
