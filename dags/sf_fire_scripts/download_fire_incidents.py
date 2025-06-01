import os
from datetime import datetime
import requests

def download_fire_incidents(**kwargs):
    # Construct the URL
    dataset_id = "wr8u-xric"
    cache_bust = "1746351254"
    # date_str = datetime.today().strftime('%Y%m%d') # Instead of using today's date, we will use Ariflow's execution date
    execution_date = kwargs['ds']   # airflow passes the execution date as a string in this format 'YYYY-MM-DD'  
    date_str = execution_date.replace('-', '')

    url = (
        f"https://data.sfgov.org/api/views/{dataset_id}/rows.csv"
        f"?fourfour={dataset_id}&cacheBust={cache_bust}&date={date_str}&accessType=DOWNLOAD"
    )

    # Define output path
    filename = f"raw_fire_incidents_{date_str}.csv"
    output_dir = "/opt/airflow/data"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    print(f"📥 Downloading from {url}")
    response = requests.get(url)
    response.raise_for_status()

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(response.text)

    print(f"✅ File saved to {output_path}")

    # Return path for next task (via XCom)
    return output_path
