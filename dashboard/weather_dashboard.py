import os
from datetime import datetime, timezone

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("WEATHER_API_URL", "http://localhost:8000").rstrip("/")
STALE_AFTER_MIN = 120

# WMO weather interpretation codes
WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Rime fog",
    51: "Light drizzle", 53: "Drizzle", 55: "Dense drizzle",
    56: "Freezing drizzle", 57: "Heavy freezing drizzle",
    61: "Light rain", 63: "Rain", 65: "Heavy rain",
    66: "Freezing rain", 67: "Heavy freezing rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow", 77: "Snow grains",
    80: "Light showers", 81: "Showers", 82: "Violent showers",
    85: "Snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm, hail", 99: "Severe thunderstorm, hail",
}

COMPASS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]

st.set_page_config(page_title="Live Airport Weather", page_icon="🌤️", layout="wide")


# ---------- helpers ----------

def api_get(path: str, params: dict | None = None):
    resp = requests.get(f"{API_URL}{path}", params=params, timeout=5)
    resp.raise_for_status()
    return resp.json()


@st.cache_data(ttl=3, show_spinner=False)
def load_airports():
    return api_get("/airports")


@st.cache_data(ttl=3, show_spinner=False)
def load_history(icao: str, hours: int):
    return api_get(f"/weather/{icao}/history", {"hours": hours})


def show(value, fmt="{:.1f}", unit=""):
    if value is None or pd.isna(value):
        return "—"
    return fmt.format(value) + unit


def compass(deg):
    if deg is None or pd.isna(deg):
        return "—"
    return COMPASS[int((deg + 11.25) // 22.5) % 16]


def condition(code):
    if code is None or pd.isna(code):
        return "—"
    return WMO_CODES.get(int(code), f"Code {int(code)}")


def visibility_label(metres):
    if metres is None or pd.isna(metres):
        return "—"
    if metres >= 8000:
        return "Good"
    if metres >= 5000:
        return "Moderate"
    if metres >= 1500:
        return "Low"
    return "Very low"


def age_text(minutes):
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return f"{int(minutes)} min ago"
    return f"{minutes / 60:.1f} h ago"


def line_chart(hist: pd.DataFrame, mapping: dict):
    cols = [c for c in mapping if c in hist.columns]
    if not cols:
        st.caption("No data for this chart yet.")
        return
    st.line_chart(hist[cols].rename(columns=mapping))


# ---------- sidebar ----------

st.title("✈️ Live Airport Weather")

with st.sidebar:
    st.header("Settings")
    refresh_s = st.select_slider("Refresh every (seconds)", [5, 10, 15, 30, 60], value=10)
    history_mins = st.select_slider("History window (mins)", [10, 20, 30, 40, 50], value=10)
    st.caption(f"API: {API_URL}")


# ---------- live view ----------

@st.fragment(run_every=refresh_s)
def live_view():
    try:
        data = load_airports()
    except requests.RequestException as exc:
        st.error(f"Cannot reach the weather API at {API_URL}: {exc}")
        return

    if not data:
        st.info("No weather data in Redis yet. Check that the weather loader is running.")
        return

    df = pd.DataFrame(data)
    df["observation_time_utc"] = pd.to_datetime(df["observation_time_utc"], utc=True)
    now = datetime.now(timezone.utc)
    df["age_min"] = (now - df["observation_time_utc"]).dt.total_seconds() / 60

    newest_age = df["age_min"].min()
    st.caption(
        f"Refreshed {now:%H:%M:%S} UTC · newest observation {age_text(newest_age)}"
    )
    if newest_age > STALE_AFTER_MIN:
        st.warning("Data looks stale: no airport has reported recently.")

    # --- KPIs ---
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Airports reporting", len(df))
    k2.metric("Average temperature", show(df["temperature_c"].mean(), unit=" °C"))

    gusts = df.dropna(subset=["wind_gusts_knots"])
    if gusts.empty:
        k3.metric("Strongest gust", "—")
    else:
        top = gusts.loc[gusts["wind_gusts_knots"].idxmax()]
        k3.metric("Strongest gust", show(top["wind_gusts_knots"], unit=" kt"), top["airport_icao"], delta_color="off")

    vis = df.dropna(subset=["visibility_m"])
    if vis.empty:
        k4.metric("Lowest visibility", "—")
    else:
        low = vis.loc[vis["visibility_m"].idxmin()]
        k4.metric("Lowest visibility", show(low["visibility_m"] / 1000, unit=" km"), low["airport_icao"], delta_color="off")

    # --- map + table ---
    left, right = st.columns([1, 1.6])

    with left:
        st.subheader("Map")
        geo = df.dropna(subset=["latitude", "longitude"])
        if geo.empty:
            st.caption("No coordinates available.")
        else:
            st.map(geo, latitude="latitude", longitude="longitude")

    with right:
        st.subheader("All airports")
        table = pd.DataFrame({
            "ICAO": df["airport_icao"],
            "City": df["city"],
            "Conditions": df["weather_code"].map(condition),
            "Temp °C": df["temperature_c"].round(1),
            "Humidity %": df["relative_humidity_pct"].round(0),
            "Wind kt": df["wind_speed_knots"].round(0),
            "Gust kt": df["wind_gusts_knots"].round(0),
            "Dir": df["wind_direction_degrees"].map(compass),
            "Visibility": df["visibility_m"].map(visibility_label),
            "Pressure hPa": df["surface_pressure_hpa"].round(0),
            "Updated": df["age_min"].map(age_text),
        })
        st.dataframe(table, hide_index=True, width="stretch")

    # --- airport detail ---
    st.divider()
    icaos = sorted(df["airport_icao"])
    icao = st.selectbox(
        "Airport",
        icaos,
        key="selected_icao",
        format_func=lambda code: f"{code} · {df.loc[df.airport_icao == code, 'airport_name'].iloc[0] or ''}",
    )
    row = df[df["airport_icao"] == icao].iloc[0]

    st.subheader(f"{row['airport_name'] or icao} ({icao})")

    m1, m2, m3, m4, m5 = st.columns(5)
    feels = row["apparent_temperature_c"]
    m1.metric(
        "Temperature",
        show(row["temperature_c"], unit=" °C"),
        None if pd.isna(feels) else f"feels {feels:.1f} °C",
        delta_color="off",
    )
    m2.metric("Conditions", condition(row["weather_code"]))
    m3.metric(
        "Wind",
        f"{show(row['wind_speed_knots'], '{:.0f}')} kt {compass(row['wind_direction_degrees'])}",
        f"gusts {show(row['wind_gusts_knots'], '{:.0f}')} kt",
        delta_color="off",
    )
    m4.metric(
        "Visibility",
        show(None if pd.isna(row["visibility_m"]) else row["visibility_m"] / 1000, unit=" km"),
        visibility_label(row["visibility_m"]),
        delta_color="off",
    )
    m5.metric("Pressure", show(row["surface_pressure_hpa"], "{:.0f}", " hPa"))

    n1, n2, n3 = st.columns(3)
    n1.metric("Humidity", show(row["relative_humidity_pct"], "{:.0f}", " %"))
    n2.metric("Cloud cover", show(row["cloud_cover_pct"], "{:.0f}", " %"))
    n3.metric("Precipitation", show(row["precipitation_mm"], "{:.1f}", " mm"))

    # --- history charts ---
    try:
        history = load_history(icao, history_mins)
    except requests.RequestException as exc:
        st.info(f"No history available for {icao}: {exc}")
        return

    if not history:
        st.info(f"No history for {icao} in the last {history_mins} h.")
        return

    hist = pd.DataFrame(history)
    hist["observation_time_utc"] = pd.to_datetime(hist["observation_time_utc"], utc=True)
    hist = hist.set_index("observation_time_utc").sort_index()

    st.caption(f"Last {history_mins} h · {len(hist)} observations (UTC)")
    t1, t2, t3, t4, t5 = st.tabs(["Temperature", "Wind", "Pressure", "Humidity & cloud", "Precipitation"])

    with t1:
        line_chart(hist, {"temperature_c": "Temperature °C", "apparent_temperature_c": "Feels like °C"})
    with t2:
        line_chart(hist, {"wind_speed_knots": "Wind kt", "wind_gusts_knots": "Gusts kt"})
    with t3:
        line_chart(hist, {"surface_pressure_hpa": "Pressure hPa"})
    with t4:
        line_chart(hist, {"relative_humidity_pct": "Humidity %", "cloud_cover_pct": "Cloud cover %"})
    with t5:
        if "precipitation_mm" in hist.columns:
            st.bar_chart(hist[["precipitation_mm"]].rename(columns={"precipitation_mm": "Precipitation mm"}))
        else:
            st.caption("No precipitation data yet.")


live_view()