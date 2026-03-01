from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'helen',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def extract():
    print("Extracting partner data...")
    partners = [
        {"partner_id": 1, "name": "Partner A", "status": "active", "country": "DE"},
        {"partner_id": 2, "name": "Partner B", "status": "inactive", "country": "US"},
        {"partner_id": 3, "name": "Partner C", "status": "active", "country": "UK"},
    ]
    print(f"Extracted {len(partners)} partner records")
    return partners

def transform():
    print("Transforming partner data...")
    transformed = [
        {"partner_id": 1, "name": "PARTNER A", "status": "active", "country": "DE"},
        {"partner_id": 3, "name": "PARTNER C", "status": "active", "country": "UK"},
    ]
    print(f"Kept {len(transformed)} active partners")
    return transformed

def load():
    print("Loading partner data into target system...")
    print("Successfully loaded 2 active partner records")

with DAG(
    dag_id='partner_etl_pipeline',
    default_args=default_args,
    description='Partner Master Data ETL Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['partner', 'mdm', 'etl'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_partners',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform_partners',
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id='load_partners',
        python_callable=load,
    )

    task_extract >> task_transform >> task_load