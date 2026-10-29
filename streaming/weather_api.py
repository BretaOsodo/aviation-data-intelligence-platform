import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

from datetime import datetime, timezone

AIRPORTS= [
    {
        "icao": "HKJK",
        "iata": "NBO",
        "name": "Jomo Kenyatta International Airport",
        "city": "Nairobi",
        "latitude": -1.3192,
        "longitude": 36.9278,
    },
	{
        "icao": "HKMO",
        "iata": "MBA",
        "name": "Moi International Airport",
        "city": "Mombasa",
        "latitude": -4.0348,
        "longitude": 39.5942,
    },
	{
        "icao": "HKKI",
        "iata": "KIS",
        "name": "Kisumu International Airport",
        "city": "Kisumu",
        "latitude": -0.0861,
        "longitude": 34.7289,
    },
	{
        "icao": "HKEL",
        "iata": "EDL",
        "name": "Eldoret International Airport",
        "city": "Eldoret",
        "latitude": 0.4045,
        "longitude": 35.2389,
    },
	{
        "icao": "HKML",
        "iata": "MYD",
        "name": "Malindi Airport",
        "city": "Malindi",
        "latitude": -3.2293,
        "longitude": 40.1017,
    },
	{
        "icao": "HKUK",
        "iata": "UKA",
        "name": "Ukunda Airport",
        "city": "Ukunda",
        "latitude": -4.2933,
        "longitude": 39.5711,
    },
	{
        "icao": "HKIS",
        "iata": "WIL",
        "name": "Wilson Airport",
        "city": "Nairobi",
        "latitude": -1.3217,
        "longitude": 36.8148,
    },
	{
        "icao": "HKKR",
        "iata": "KEY",
        "name": "Kericho Airport",
        "city": "Kericho",
        "latitude": -0.4169,
        "longitude": 35.2420,
    },
]

CURRENT_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "is_day",
    "precipitation",
    "rain",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "surface_pressure",
    "visibility",
]

# Ste up the open_meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache',expire_after=3600)
retry_session= retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo= openmeteo_requests.Client(session=retry_session)

url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude":  [a["latitude"] for a in AIRPORTS],
	"longitude": [a["longitude"] for a in AIRPORTS],
	"current": CURRENT_VARIABLES,
	"timezone": "Africa/Nairobi",
	"wind_speed_unit":"kn"
}

responses = openmeteo.weather_api(url, params=params)

if len(responses) != len(AIRPORTS):
	raise RuntimeError(
		f"Requested {len(AIRPORTS)} airports but recieved"
		f"{len(responses)} responses"
	)

# Helper to read current weather variables

def current_value(current, variable_names, variable_name):
	"""Get a currebt weather value by its requested variable name"""
	index= variable_names.index(variable_name)
	return current.Variables(index).Value()

#process each airport
current_weather_rows = []
hourly_weather_rows = []

for airport, response in zip(AIRPORTS, responses):
    print("\n" + "=" * 60)
    print(f"{airport['icao']} | {airport['name']}")
    print(f"City: {airport['city']}")
    print(
        f"Coordinates: {response.Latitude():.4f}, "
        f"{response.Longitude():.4f}"
    )
    print(f"Elevation: {response.Elevation()} m")
    print(f"Timezone: Africa/Nairobi")
    print(f"UTC offset: {response.UtcOffsetSeconds()} seconds")

    # ------------------------------
    # Current conditions
    # ------------------------------

    current = response.Current()

    temperature = current_value(
        current, CURRENT_VARIABLES, "temperature_2m"
    )
    humidity = current_value(
        current, CURRENT_VARIABLES, "relative_humidity_2m"
    )
    apparent_temperature = current_value(
        current, CURRENT_VARIABLES, "apparent_temperature"
    )
    precipitation = current_value(
        current, CURRENT_VARIABLES, "precipitation"
    )
    rain = current_value(
        current, CURRENT_VARIABLES, "rain"
    )
    weather_code = current_value(
        current, CURRENT_VARIABLES, "weather_code"
    )
    cloud_cover = current_value(
        current, CURRENT_VARIABLES, "cloud_cover"
    )
    wind_speed = current_value(
        current, CURRENT_VARIABLES, "wind_speed_10m"
    )
    wind_direction = current_value(
        current, CURRENT_VARIABLES, "wind_direction_10m"
    )
    wind_gusts = current_value(
        current, CURRENT_VARIABLES, "wind_gusts_10m"
    )
    pressure = current_value(
        current, CURRENT_VARIABLES, "surface_pressure"
    )
    visibility = current_value(
        current, CURRENT_VARIABLES, "visibility"
    )

    current_time = datetime.fromtimestamp(
        current.Time(), tz=timezone.utc
    ).isoformat()

    print(f"Observation time: {current_time}")
    print(f"Temperature: {temperature:.1f} °C")
    print(f"Humidity: {humidity:.0f}%")
    print(f"Apparent temperature: {apparent_temperature:.1f} °C")
    print(f"Precipitation: {precipitation:.1f} mm")
    print(f"Rain: {rain:.1f} mm")
    print(f"Weather code: {weather_code}")
    print(f"Cloud cover: {cloud_cover:.0f}%")
    print(f"Wind speed: {wind_speed:.1f} knots")
    print(f"Wind direction: {wind_direction:.0f}°")
    print(f"Wind gusts: {wind_gusts:.1f} knots")
    print(f"Surface pressure: {pressure:.1f} hPa")
    print(f"Visibility: {visibility:.0f} m")

    current_weather_rows.append({
        "airport_icao": airport["icao"],
        "airport_iata": airport["iata"],
        "airport_name": airport["name"],
        "city": airport["city"],
        "latitude": response.Latitude(),
        "longitude": response.Longitude(),
        "elevation_m": response.Elevation(),
        "observation_time_utc": current_time,
        "temperature_c": temperature,
        "relative_humidity_pct": humidity,
        "apparent_temperature_c": apparent_temperature,
        "precipitation_mm": precipitation,
        "rain_mm": rain,
        "weather_code": weather_code,
        "cloud_cover_pct": cloud_cover,
        "wind_speed_knots": wind_speed,
        "wind_direction_degrees": wind_direction,
        "wind_gusts_knots": wind_gusts,
        "surface_pressure_hpa": pressure,
        "visibility_m": visibility,
    })
