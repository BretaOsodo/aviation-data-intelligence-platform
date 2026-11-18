
"""Read airport weather data back out of ClickHouse.

Examples:
    python read_weather_from_clickhouse.py latest --airport HKJK --limit 10
    python read_weather_from_clickhouse.py history --airport HKJK --hours 48
    python read_weather_from_clickhouse.py daily --airport HKJK --days 7
    python read_weather_from_clickhouse.py history --hours 24 --csv out.csv

Settings come from environment variables (or a .env file):
    CLICKHOUSE_HOST         default: localhost
    CLICKHOUSE_HTTP_PORT    default: 8123
    CLICKHOUSE_USER         (or CLICKHOUSE_ADMIN)
    CLICKHOUSE_PASSWORD
    CLICKHOUSE_DATABASE     default: aviation
"""
import argparse
import os
import sys

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError
from dotenv import load_dotenv

load_dotenv()

# The view applies FINAL, so duplicates from retried batches are collapsed.
TABLE = "airport_weather_latest"


def get_client():
    user = os.getenv("CLICKHOUSE_USER") or os.getenv("CLICKHOUSE_ADMIN")
    password = os.getenv("CLICKHOUSE_PASSWORD")

    if not user or not password:
        sys.exit(
            "Set CLICKHOUSE_USER (or CLICKHOUSE_ADMIN) and "
            "CLICKHOUSE_PASSWORD in the environment or a .env file."
        )

    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=user,
        password=password,
        database=os.getenv("CLICKHOUSE_DATABASE", "aviation"),
        connect_timeout=10,
    )


def latest(client, airport, limit):
    """Most recent observations for one airport."""
    return client.query_df(
        f"""
        SELECT airport_icao, observation_time_utc, temperature_c,
               relative_humidity_pct, wind_speed_knots, wind_gusts_knots,
               wind_direction_degrees, surface_pressure_hpa, visibility_m,
               cloud_cover_pct, precipitation_mm
        FROM {TABLE}
        WHERE airport_icao = {{airport:String}}
        ORDER BY observation_time_utc DESC
        LIMIT {{limit:UInt32}}
        """,
        parameters={"airport": airport, "limit": limit},
    )


def history(client, airport, hours):
    """Every observation for one airport in the last N hours."""
    return client.query_df(
        f"""
        SELECT *
        FROM {TABLE}
        WHERE airport_icao = {{airport:String}}
          AND observation_time_utc >= now() - INTERVAL {{hours:UInt32}} HOUR
        ORDER BY observation_time_utc
        """,
        parameters={"airport": airport, "hours": hours},
    )


def daily_summary(client, airport, days):
    """One row per day: temperature range, strongest gust, mean pressure."""
    return client.query_df(
        f"""
        SELECT toDate(observation_time_utc)      AS day,
               count()                           AS observations,
               round(min(temperature_c), 1)      AS min_temp_c,
               round(avg(temperature_c), 1)      AS avg_temp_c,
               round(max(temperature_c), 1)      AS max_temp_c,
               round(max(wind_gusts_knots), 1)   AS max_gust_knots,
               round(avg(surface_pressure_hpa), 1) AS avg_pressure_hpa,
               round(sum(precipitation_mm), 1)   AS total_precip_mm
        FROM {TABLE}
        WHERE airport_icao = {{airport:String}}
          AND observation_time_utc >= now() - INTERVAL {{days:UInt32}} DAY
        GROUP BY day
        ORDER BY day
        """,
        parameters={"airport": airport, "days": days},
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--csv", help="also save the result to this CSV file")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("latest", help="most recent observations")
    p.add_argument("--airport", default="HKJK")
    p.add_argument("--limit", type=int, default=10)

    p = sub.add_parser("history", help="all observations in the last N hours")
    p.add_argument("--airport", default="HKJK")
    p.add_argument("--hours", type=int, default=24)

    p = sub.add_parser("daily", help="daily summary for the last N days")
    p.add_argument("--airport", default="HKJK")
    p.add_argument("--days", type=int, default=7)

    args = parser.parse_args()

    try:
        client = get_client()

        if args.command == "latest":
            df = latest(client, args.airport, args.limit)
        elif args.command == "history":
            df = history(client, args.airport, args.hours)
        else:
            df = daily_summary(client, args.airport, args.days)
    except ClickHouseError as exc:
        # Keep the message short; the full text can include the URL only,
        # never the password.
        sys.exit(
            f"ClickHouse request failed: {str(exc)[:300]}\n"
            "Check CLICKHOUSE_HOST and CLICKHOUSE_HTTP_PORT (is port 8123 "
            "published by Docker?), the user/password, and the database name."
        )

    if df.empty:
        print("No rows returned. Check the airport code and the time range.")
    else:
        print(df.to_string(index=False))
        print(f"\n{len(df)} row(s)")

    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"Saved to {args.csv}")


if __name__ == "__main__":
    main()