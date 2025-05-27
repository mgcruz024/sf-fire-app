import json
import os
import openmeteo_requests
import pandas as pd
import requests_cache
import psycopg2
from retry_requests import retry
from sqlalchemy import create_engine

# PostgreSQL config
db_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sf_fire_dev",
    "user": "postgres",
    "password": "password"
}

table_name = "meteoStationWeather"

# Setup the Open-Meteo API client with cache and retry
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Load the geo boundaries JSON
geo_path = os.path.join("Data", "AQI CN", "Geo Boundaries", "AQI_CN_GeoBoundaries.json")
with open(geo_path, "r") as f:
    geo_json = json.load(f)
geo_data = geo_json["data"]

# Prepare PostgreSQL connection string
engine = create_engine(
    f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}'
    f'@{db_config["host"]}:{db_config["port"]}/{db_config["dbname"]}'
)

# Loop through each station
for station in geo_data:
    station_id = station["uid"]
    lat = station["lat"]
    lon = station["lon"]

    print(f"Fetching data for Station {station_id} at {lat}, {lon}")

    # Set API parameters
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": [
            "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max",
            "apparent_temperature_min", "daylight_duration", "sunshine_duration",
            "uv_index_max", "uv_index_clear_sky_max", "wind_speed_10m_max",
            "wind_gusts_10m_max", "wind_direction_10m_dominant", "rain_sum",
            "weather_code", "showers_sum", "precipitation_sum", "precipitation_hours"
        ],
        "timezone": "America/Los_Angeles",
        "past_days": 7,
        "forecast_days": 1,
        "wind_speed_unit": "mph",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch"
    }

    try:
        # Fetch the data
        responses = openmeteo.weather_api("https://api.open-meteo.com/v1/forecast", params=params)
        response = responses[0]
        daily = response.Daily()

        # Build the dataframe
        daily_data = {"date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )}
        daily_data["station_id"] = station_id  # Add station ID

        # Add weather variables
        variable_names = [
            "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max",
            "apparent_temperature_min", "daylight_duration", "sunshine_duration",
            "uv_index_max", "uv_index_clear_sky_max", "wind_speed_10m_max",
            "wind_gusts_10m_max", "wind_direction_10m_dominant", "rain_sum",
            "weather_code", "showers_sum", "precipitation_sum", "precipitation_hours"
        ]

        for idx, var_name in enumerate(variable_names):
            daily_data[var_name] = daily.Variables(idx).ValuesAsNumpy()

        daily_df = pd.DataFrame(daily_data)

        # Load into PostgreSQL
        daily_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Loaded data for Station {station_id} into {table_name}")

    except Exception as e:
        print(f"Error processing Station {station_id}: {e}")

print("✅ All stations processed.")
