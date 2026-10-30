import json
import math
from pathlib import Path
from datetime import datetime

from fastavro import parse_schema, validation

SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent / 'schema' / 'weather.avsc'
)

with open(SCHEMA_PATH,encoding='utf-8')as file:
    WEATHER_SCHEMA=parse_schema(json.load(file))

def validate_weather(record):

    errors = []

    # 1. Validate the structure and data types against avro
    try:
        validation.validate(
            record,
            schema=WEATHER_SCHEMA,
            raise_errors=True
        )
    except Exception as exc:
        errors.append(f"Avro schema validation failed:{exc}")
        return errors

    #2. ensure numeric measurements are finite
    numeric_fields=[
        "latitude", "longitude", "elevation_m",
        "temperature_c", "relative_humidity_pct",
        "apparent_temperature_c", "precipitation_mm",
        "rain_mm", "weather_code", "cloud_cover_pct",
        "wind_speed_knots", "wind_direction_degrees",
        "wind_gusts_knots", "surface_pressure_hpa",
        "visibility_m"
    ]

    for field in numeric_fields:
        value= record[field]
        if not math.isfinite(value):
            errors.append(f"{field} must be a finite number")

    #Run range checks only when numeric values are finite
    def finite(field):
        value= record[field]
        return math.isfinite(value)

    if finite("latitude") and not -90 <= record["latitude"] <= 90:
        errors.append(f"{field} must be between -90 and 90")

    if finite ("longitude") and not -180 <= record["longitude"] <= 180:
        errors.append(f"{field} must be between -180 and 180")

    if finite("relative_humidity_pct")  and not(
        0<= record["relative_humidity_pct"] <= 100
    ):
        errors.append(f"{field} must be between 0 and 100")

    if finite("cloud_cover_pct") and not(
        0<= record["cloud_cover_pct"] <= 100
    ):
        errors.append(f"{field} must be between 0 and 100")

    if finite("wind_direction_degrees") and not (
            0 <= record["wind_direction_degrees"] <= 360
    ):
        errors.append("wind_direction_degrees must be between 0 and 360")


    for field  in (
            "precipitation_mm", "rain_mm",
            "wind_speed_knots", "wind_gusts_knots",
            "visibility_m"
    ):
        if finite(field) and record[field]< 0:
            errors.append(f"{field} cannot be negative")

    #check observations timestamp format
    try:
        datetime.fromisoformat(
            record["observation_time_utc"].replace("Z", "+00:00")
        )
    except ValueError:
        errors.append(f"{field}must be a valid ISO timestamp")

    return errors