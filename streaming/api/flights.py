
import os
import json
import time
import logging
import requests

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY")
API_URL = "https://api.aviationstack.com/v1/flights"

KENYA_AIRPORTS = [
    "NBO"
]

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

def make_request(session, params, airport_code, filter_name):
    """Make an Aviationstack request with controlled retries."""

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                API_URL,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            # Rate limit: do not keep sending requests.
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")

                raise RuntimeError(
                    "Aviationstack rate limit reached "
                    f"for {filter_name}={airport_code}. "
                    f"Retry-After: {retry_after or 'not provided'}. "
                    "Check your API usage quota."
                )

            # Retry temporary server errors.
            if response.status_code in (500, 502, 503, 504):
                response.raise_for_status()

            response.raise_for_status()

            payload = response.json()

            if payload.get("error"):
                raise RuntimeError(
                    f"Aviationstack API returned an error "
                    f"for {filter_name}={airport_code}: "
                    f"{payload['error']}"
                )

            return payload

        except requests.exceptions.HTTPError:
            # Do not retry client errors such as 400 or 401.
            status = (
                response.status_code
                if "response" in locals()
                else None
            )

            if status not in (500, 502, 503, 504):
                raise

            if attempt == MAX_RETRIES - 1:
                raise

            wait_seconds = 2 ** (attempt + 1)

            logger.warning(
                "Aviationstack server error. "
                "Retrying in %s seconds.",
                wait_seconds
            )

            time.sleep(wait_seconds)

        except requests.exceptions.RequestException:
            if attempt == MAX_RETRIES - 1:
                raise

            wait_seconds = 2 ** (attempt + 1)

            logger.warning(
                "Temporary network error. Retrying in %s seconds.",
                wait_seconds
            )

            time.sleep(wait_seconds)


def get_flight_key(flight):
    """Generate a composite key for flight deduplication."""

    details = flight.get("flight") or {}
    departure = flight.get("departure") or {}
    arrival = flight.get("arrival") or {}
    airline = flight.get("airline") or {}

    return (
        flight.get("flight_date"),
        airline.get("icao") or airline.get("iata"),
        details.get("icao") or details.get("iata") or details.get("number"),
        departure.get("iata"),
        arrival.get("iata"),
        departure.get("scheduled") or departure.get("actual"),
    )


def get_airports():

    if not API_KEY:
        raise ValueError("API_KEY environment variable not set")

    unique_flights = {}

    with requests.Session() as session:

        for airport_code in KENYA_AIRPORTS:

            for filter_name in ("dep_iata", "arr_iata"):

                params = {
                    "access_key": API_KEY,
                    filter_name: airport_code,
                    "limit": 100,
                }

                try:
                    payload = make_request(
                        session,
                        params,
                        airport_code,
                        filter_name
                    )

                    records = payload.get("data", [])

                    logger.info(
                        "%s=%s: %s flights returned",
                        filter_name,
                        airport_code,
                        len(records)
                    )

                    for flight in records:

                        flight_key = get_flight_key(flight)

                        unique_flights[str(flight_key)] = flight

                except Exception:
                    logger.exception(
                        "Flight API request failed for %s=%s",
                        filter_name,
                        airport_code
                    )
                    raise

    flights = list(unique_flights.values())

    logger.info(
        "Total unique flights collected: %s",
        len(flights)
    )

    return flights


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    flights = get_airports()

    print(
        json.dumps(
            flights,
            indent=4,
            allow_nan=False
        )
    )
