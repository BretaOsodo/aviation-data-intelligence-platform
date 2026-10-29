from streaming import weather_api
import json

data = weather_api.convert_to_json()

def test_weather_api():
    parsed_data = json.loads(data)

    assert isinstance(parsed_data, list)
    assert len(parsed_data) > 0
    assert isinstance(parsed_data[0], dict)