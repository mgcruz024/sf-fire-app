
import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import create_engine
from io import StringIO

# -------------------------------
# 🔧 Configuration
# -------------------------------
dataset_id = "wr8u-xric"
cache_bust = "1746351254"
today_str = datetime.today().strftime('%Y%m%d')
url = f"https://data.sfgov.org/api/views/{dataset_id}/rows.csv?fourfour={dataset_id}&cacheBust={cache_bust}&date={today_str}&accessType=DOWNLOAD"

db_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sf_fire_dev",
    "user": "postgres",
    "password": "password"
}


# -------------------------------
# 📥 Download CSV
# -------------------------------
print("Downloading data from Open Data portal...")
response = requests.get(url)
if response.status_code != 200:
    raise RuntimeError(f"Failed to download file: {response.status_code}")

# Load to DataFrame
df_full = pd.read_csv(StringIO(response.text))
df_full["data_loaded_at"] = datetime.now()

# -------------------------------
# 📦 Subset definitions
# -------------------------------
subset_definitions = {

    "openSfFireIncident": list(df_full.columns),  # store the full dataset too
    "openSfIncidentInfo": [
        "Incident Number", "Exposure Number", "ID", "Call Number", "Incident Date",
        "Alarm DtTm", "Arrival DtTm", "Close DtTm", "Number of Alarms", "Primary Situation",
        "Mutual Aid", "Action Taken Primary", "Action Taken Secondary", "Action Taken Other",
        "data_loaded_at"
    ],
    "openSfLocation": [
        "Incident Number", "Address", "City", "zipcode", "Battalion", "Station Area",
        "Box", "Supervisor District", "neighborhood_district", "point", "data_loaded_at"
    ],
    "openSfPersonnel": [
        "Incident Number", "Suppression Units", "Suppression Personnel",
        "EMS Units", "EMS Personnel", "Other Units", "Other Personnel", "First Unit On Scene",
        "data_loaded_at"
    ],
    "openSfLossCasualty": [
        "Incident Number", "Estimated Property Loss", "Estimated Contents Loss",
        "Fire Fatalities", "Fire Injuries", "Civilian Fatalities", "Civilian Injuries",
        "data_loaded_at"
    ],
    "openSfFireCause": [
        "Incident Number", "Property Use", "Area of Fire Origin", "Ignition Cause",
        "Ignition Factor Primary", "Ignition Factor Secondary", "Heat Source",
        "Item First Ignited", "Human Factors Associated with Ignition", "data_loaded_at"
    ],
    "openSfStructureDetail": [
        "Incident Number", "Structure Type", "Structure Status", "Floor of Fire Origin",
        "Fire Spread", "No Flame Spread", "Number of floors with minimum damage",
        "Number of floors with significant damage", "Number of floors with heavy damage",
        "Number of floors with extreme damage", "data_loaded_at"
    ],
    "openSfDetector": [
        "Incident Number", "Detectors Present", "Detector Type", "Detector Operation",
        "Detector Effectiveness", "Detector Failure Reason", "data_loaded_at"
    ],
    "openSfSuppression": [
        "Incident Number", "Automatic Extinguishing System Present",
        "Automatic Extinguishing Sytem Type", "Automatic Extinguishing Sytem Perfomance",
        "Automatic Extinguishing Sytem Failure Reason", "Number of Sprinkler Heads Operating",
        "data_loaded_at"
    ]
}

# -------------------------------
# 🗃️ PostgreSQL Upload
# -------------------------------
engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
)

for table_name, columns in subset_definitions.items():
    try:
        df_subset = df_full[columns].copy()
        df_subset.to_sql(table_name, engine, schema='public', if_exists='append', index=False)
        print(f"✅ Loaded {table_name} to PostgreSQL.")
    except KeyError as e:
        print(f"⚠️ Missing columns for {table_name}: {e}")
    except Exception as e:
        print(f"❌ Error loading {table_name}: {e}")
