from streaming.api import weather_api
import json

data = weather_api.get_weather_data()

def test_weather_api():
    parsed_data = data

    assert isinstance(parsed_data, list)
    assert len(parsed_data) > 0
    assert isinstance(parsed_data[0], dict)