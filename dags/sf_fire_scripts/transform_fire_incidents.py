import os
import pandas as pd

# Define subset columns 
subset_definitions = {
    "openSfFireIncident": None,  # keep full dataset
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

def transform_fire_incidents(raw_file_path, **kwargs):
    execution_date = kwargs['ds']  # or kwargs['execution_date']
    date_str = execution_date.replace('-', '')
    df_full = pd.read_csv(raw_file_path)

    base_output_dir = "/opt/airflow/data/transformed"
    os.makedirs(base_output_dir, exist_ok=True)

    for subset_name, columns in subset_definitions.items():
        if columns is None:
            df_subset = df_full.copy()
        else:
            df_subset = df_full[columns].copy()
        
        subset_file = os.path.join(base_output_dir, f"{subset_name}_{date_str}.csv")
        df_subset.to_csv(subset_file, index=False)
        print(f"✅ Saved {subset_name} to {subset_file}")

    return base_output_dir
