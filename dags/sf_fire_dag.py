from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sf_fire_scripts.download_fire_incidents import download_fire_incidents
from sf_fire_scripts.transform_fire_incidents import transform_fire_incidents


default_args = {
    'owner': 'miguel',
    'retries': 2,
    'retry_delay': 300  # seconds (5 minutes)
}

with DAG(
    dag_id='sf_fire_daily_loader',
    default_args=default_args,
    description='DAG to download, transform, and load SF Fire Incidents data',
    start_date=datetime(2025, 5, 1),
    schedule_interval='@daily',
    catchup=False # This tells Airflow to run all missed intervals since start_date
) as dag:

    download_task = PythonOperator(
        task_id='download_fire_incidents',
        python_callable=download_fire_incidents,
        provide_context=True  # this allows Airflow to pass **kwargs
    )

    transform_task = PythonOperator(
        task_id='transform_fire_incidents',
        python_callable=transform_fire_incidents,
        op_args=[
            "{{ ti.xcom_pull(task_ids='download_fire_incidents') }}"
        ],
        provide_context=True  # ensures kwargs includes 'ds' and others
    )

    download_task >> transform_task
