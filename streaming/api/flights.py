import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()


API_KEY = os.getenv("API_KEY")
API_URL ="https://api.aviationstack.com/v1/flights"
# Kenya Airport IATA codes

KENYA_AIRPORTS=[
    "NBO",  # Jomo Kenyatta International Airport
    "WIL",  # Wilson Airport
    "MBA",  # Moi International Airport
    "KIS",  # Kisumu International Airport
    "EDL",  # Eldoret International Airport
    "MYD",  # Malindi Airport
    "UKA",  # Ukunda Airport
]
def get_airports():
    if not API_KEY:
        raise ValueError("API_KEY environment variable not set")
    unique_flights = {}
    try:
        try:
            for airport_codes in KENYA_AIRPORTS:
                for filter_name in ("dep_iata", "arr_iata"):

                    params = {
                        "access_key": API_KEY,
                        filter_name: airport_codes,
                        "limit": 100
                    }

                    response = requests.get(
                        API_URL,
                        params=params,
                        timeout= 30
                    )
                    response.raise_for_status()

                    payload = response.json()

                    if "error" in payload:
                        raise RuntimeError(
                            f"AviationStack API error: {payload['error']}"
                        )

                    records = payload.get("data",[])
                    print(
                        f"{filter_name}={airport_codes}: "
                        f"{len(records)} flights returned"
                    )

                    for flight in records:
                        flight_details=flight.get("flight")or {}
                        departure = flight.get("departure") or {}
                        arrival = flight.get("arrival") or {}

                        #deduplicate flights returned by multiple queries
                        flight_id = (
                            flight_details.get("icao")
                            or flight_details.get("iata")
                        )

                        #use composite key if flight identfiers are absent

                        if not flight_id:
                            flight_id = (
                                flight.get("flight_date"),
                                departure.get("iata"),
                                arrival.get("iata"),
                                departure.get("scheduled")
                            )

                        unique_flights[str(flight_id)] = flight

            return list(unique_flights.values())
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to fetch flight data: {e}") from e
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Request failed for {airport_codes}"
            f"({filter_name}:{e})"
        )

if __name__ == "__main__":
    flights = get_airports()

    flight_data = json.dumps(flights, indent=4,allow_nan=False)

    print(flight_data)