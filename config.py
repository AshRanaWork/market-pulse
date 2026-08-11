"""Market Pulse configuration.
Adding a market = adding one entry here. Nothing else changes.
"""

MARKETS = {
    "LAS": {
        "icao": "KLAS",
        "name": "Las Vegas - Harry Reid International",
        "lat": 36.08,
        "lon": -115.15,
        "tz": "America/Los_Angeles",
    },
    "LAX": {
        "icao": "KLAX",
        "name": "Los Angeles International",
        "lat": 33.94,
        "lon": -118.41,
        "tz": "America/Los_Angeles",
    },
}

# Raw CSV column orders (no header rows are written; these ARE the schema).
FLIGHTS_COLUMNS = [
    "market", "icao24", "callsign", "departure_airport",
    "arrival_ts_utc", "arrival_date_local", "arrival_hour_local",
    "extracted_at_utc",
]
WEATHER_COLUMNS = [
    "market", "observed_at_local", "observed_date_local", "hour_local",
    "temp_f", "precip_mm", "wind_kmh", "extracted_at_utc",
]
