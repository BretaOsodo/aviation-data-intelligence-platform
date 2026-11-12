import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Optional

import redis.asyncio as redis
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
MAX_HISTORY_HOURS = int(os.getenv("HISTORY_HOURS", "24"))

# Must match the keys written by load_weather_to_redis.py
AIRPORTS_KEY = "weather:airports"
LATEST_PREFIX = "weather:latest:"
HISTORY_PREFIX = "weather:history:"

ICAO_PATTERN = r"^[A-Za-z]{4}$"

logger = logging.getLogger("weather-api")


class Observation(BaseModel):
    observation_time_utc: datetime
    temperature_c: Optional[float] = None
    relative_humidity_pct: Optional[float] = None
    apparent_temperature_c: Optional[float] = None
    precipitation_mm: Optional[float] = None
    rain_mm: Optional[float] = None
    weather_code: Optional[int] = None
    cloud_cover_pct: Optional[float] = None
    wind_speed_knots: Optional[float] = None
    wind_direction_degrees: Optional[float] = None
    wind_gusts_knots: Optional[float] = None
    surface_pressure_hpa: Optional[float] = None
    visibility_m: Optional[float] = None


class AirportWeather(Observation):
    airport_icao: str
    airport_iata: Optional[str] = None
    airport_name: Optional[str] = None
    city: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation_m: Optional[float] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
    )
    yield
    await app.state.redis.aclose()


app = FastAPI(
    title="Airport Weather API",
    description="Latest and recent weather per airport, served from Redis.",
    version="1.0.0",
    lifespan=lifespan,
)

# Read-only API, so open CORS is fine for a dashboard on another origin.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis


def _parse_airport(row: dict) -> Optional[AirportWeather]:
    try:
        return AirportWeather.model_validate(row)
    except ValidationError:
        logger.exception("Skipping malformed weather record: %s", row)
        return None


@app.get("/health")
async def health(r: redis.Redis = Depends(get_redis)):
    try:
        await r.ping()
        airports = await r.zcard(AIRPORTS_KEY)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {exc}")
    return {"status": "ok", "airports": airports}


@app.get("/airports", response_model=list[AirportWeather])
async def list_airports(r: redis.Redis = Depends(get_redis)):
    """Latest observation for every airport currently in Redis."""
    icaos = await r.zrange(AIRPORTS_KEY, 0, -1)
    if not icaos:
        return []

    async with r.pipeline(transaction=False) as pipe:
        for icao in icaos:
            pipe.hgetall(f"{LATEST_PREFIX}{icao}")
        rows = await pipe.execute()

    # An empty hash means the key expired but the index entry has not yet.
    airports = [_parse_airport(row) for row in rows if row]
    return sorted((a for a in airports if a), key=lambda a: a.airport_icao)


@app.get("/weather/{icao}", response_model=AirportWeather)
async def get_weather(
    icao: Annotated[str, Path(pattern=ICAO_PATTERN)],
    r: redis.Redis = Depends(get_redis),
):
    row = await r.hgetall(f"{LATEST_PREFIX}{icao.upper()}")
    if not row:
        raise HTTPException(status_code=404, detail=f"No weather for {icao.upper()}")
    airport = _parse_airport(row)
    if airport is None:
        raise HTTPException(status_code=500, detail="Stored record is malformed")
    return airport


@app.get("/weather/{icao}/history", response_model=list[Observation])
async def get_history(
    icao: Annotated[str, Path(pattern=ICAO_PATTERN)],
    hours: int = Query(6, ge=1, le=MAX_HISTORY_HOURS),
    r: redis.Redis = Depends(get_redis),
):
    """Observations from the last `hours`, measured back from the newest one."""
    key = f"{HISTORY_PREFIX}{icao.upper()}"

    newest = await r.zrevrange(key, 0, 0, withscores=True)
    if not newest:
        raise HTTPException(status_code=404, detail=f"No history for {icao.upper()}")

    newest_score = newest[0][1]
    payloads = await r.zrangebyscore(key, newest_score - hours * 3600, "+inf")
    return [Observation.model_validate_json(p) for p in payloads]