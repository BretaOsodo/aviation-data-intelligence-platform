from streaming.api import flights
import json

data = flights.flight_data

def test_flights_data():
    assert isinstance(data, list)
    assert isinstance(data[0], dict)
    assert len(data) > 0