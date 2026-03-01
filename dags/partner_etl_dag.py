from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'helen',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def extract(**context):
    print("Extracting partner data...")
    partners = [
        {"partner_id": 1, "name": "Partner A", "status": "active", "country": "DE"},
        {"partner_id": 2, "name": "Partner B", "status": "inactive", "country": "US"},
        {"partner_id": 3, "name": "Partner C", "status": "active", "country": "UK"},
    ]
    print(f"Extracted {len(partners)} partner records")
    context['ti'].xcom_push(key='raw_partners', value=partners)

def transform(**context):
    partners = context['ti'].xcom_pull(task_ids='extract_partners', key='raw_partners')
    print(f"Transforming {len(partners)} partner records...")
    transformed = [
        {"partner_id": p["partner_id"], "name": p["name"].upper(), "country": p["country"]}
        for p in partners if p["status"] == "active"
    ]
    print(f"Kept {len(transformed)} active partners")
    context['ti'].xcom_push(key='transformed_partners', value=transformed)

def load(**context):
    transformed = context['ti'].xcom_pull(task_ids='transform_partners', key='transformed_partners')
    print(f"Loading {len(transformed)} partner records into target system...")
    print(f"Successfully loaded {len(transformed)} active partner records")

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
        provide_context=True,
    )

    task_transform = PythonOperator(
        task_id='transform_partners',
        python_callable=transform,
        provide_context=True,
    )

    task_load = PythonOperator(
        task_id='load_partners',
        python_callable=load,
        provide_context=True,
    )

    task_extract >> task_transform >> task_load