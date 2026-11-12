from streaming.api import flights
import json
import os
import pytest

requires_api_key = pytest.mark.skipif(
    not os.environ.get('API_KEY'), reason='API key not set; Live API test'
)

@pytest.fixture(scope='module')
def data():
    return flights.get_airports()
@requires_api_key
def test_airports_has_expected_fields(data):
    assert isinstance(data, list)
    assert isinstance(data[0], dict)
    assert len(data) > 0