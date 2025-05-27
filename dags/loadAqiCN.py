import os
import json
import requests
import psycopg2
from datetime import datetime

# Database config
db_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sf_fire_dev",
    "user": "postgres",
    "password": "password"
}

table_name = "aqicnStationAQI"
aqi_token = "9a9ab252d291fd00be2688205b8a917591477de1"

# Path to station list
geo_path = os.path.join("Data", "AQI CN", "Geo Boundaries", "AQI_CN_GeoBoundaries.json")

# Load geo boundary station list
with open(geo_path, "r", encoding="utf-8") as f:
    stations = json.load(f)["data"]

# Postgres insert function
def insert_record(conn, record):
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {table_name} (
                station_id,
                aqi,
                station_lat,
                station_lng,
                station_name,
                station_location,
                station_network,
                dominent_pol,
                date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record["station_id"],
            record["aqi"],
            record["station_lat"],
            record["station_lng"],
            record["station_name"],
            record["station_location"],
            record["station_network"],
            record["dominent_pol"],
            record["date"]
        ))

# Transform API response to schema
def extract_fields(data):
    idx = data.get("idx")
    return {
        "station_id": idx,
        "aqi": data.get("aqi"),
        "station_lat": data.get("city", {}).get("geo", [None, None])[0],
        "station_lng": data.get("city", {}).get("geo", [None, None])[1],
        "station_name": data.get("city", {}).get("name"),
        "station_location": data.get("city", {}).get("location"),
        "station_network": "Citizen" if idx is not None and idx < 0 else "Official",
        "dominent_pol": data.get("dominentpol"),
        "date": data.get("time", {}).get("s")
    }

# Connect to DB
conn = psycopg2.connect(**db_config)
conn.autocommit = True

# Loop through stations and load into Postgres
for station in stations:
    uid = station.get("uid")
    url = f"http://api.waqi.info/feed/@{uid}/?token={aqi_token}"

    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json().get("data", {})
        record = extract_fields(data)
        insert_record(conn, record)
        print(f"Inserted station_id {record['station_id']}")
    except Exception as e:
        print(f"Failed to load UID {uid}: {e}")

conn.close()
