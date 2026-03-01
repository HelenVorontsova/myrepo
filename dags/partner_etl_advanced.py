"""
Advanced Airflow DAG - Partner ETL Pipeline
Demonstrates: Idempotency, Backfill/Catchup, Retries, XCom, Best Practices

Author: Helen
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ============================================================
# DEFAULT ARGS - Best Practice: always define these
# ============================================================
default_args = {
    'owner': 'helen',                        # who owns this DAG
    'depends_on_past': False,                # don't wait for previous run to succeed
    'email_on_failure': False,               # set True in production with real email
    'email_on_retry': False,
    'retries': 3,                            # retry 3 times on failure
    'retry_delay': timedelta(minutes=5),     # wait 5 min between retries
    'retry_exponential_backoff': True,       # wait longer each retry: 5m, 10m, 20m
    'execution_timeout': timedelta(hours=1), # kill task if it runs over 1 hour
}


# ============================================================
# TASK 1: EXTRACT
# Best Practice: use execution_date for IDEMPOTENCY
# Idempotency = running same task twice gives same result
# ============================================================
def extract_partners(**context):
    """
    IDEMPOTENCY EXAMPLE:
    We use execution_date (the logical date of the run) NOT today's date.
    This means if we re-run a past date, we always get the same data for that date.
    
    BAD (not idempotent):
        date = datetime.now()  # different result every time!
    
    GOOD (idempotent):
        date = context['execution_date']  # always same for same run
    """
    execution_date = context['execution_date']
    ds = context['ds']  # execution date as string: '2024-01-15'
    
    logger.info(f"Extracting partner data for date: {ds}")
    
    # Simulate fetching data for a specific date partition
    partners = [
        {"partner_id": 1, "name": "Partner A", "status": "active",   "country": "DE", "date": ds},
        {"partner_id": 2, "name": "Partner B", "status": "inactive", "country": "US", "date": ds},
        {"partner_id": 3, "name": "Partner C", "status": "active",   "country": "UK", "date": ds},
    ]
    
    logger.info(f"Extracted {len(partners)} partner records for {ds}")
    
    # Push data to XCom so next task can use it
    # XCom = "cross-communication" between tasks
    context['ti'].xcom_push(key='raw_partners', value=partners)
    
    return len(partners)


# ============================================================
# TASK 2: VALIDATE
# Best Practice: always validate before transforming
# ============================================================
def validate_partners(**context):
    """
    RETRY EXAMPLE:
    This task has retries=3. If it fails (e.g. bad data), Airflow retries it.
    With retry_exponential_backoff=True it waits: 5min, 10min, 20min between retries.
    """
    # Pull data from previous task via XCom
    partners = context['ti'].xcom_pull(task_ids='extract_partners', key='raw_partners')
    
    logger.info(f"Validating {len(partners)} records...")
    
    errors = []
    for p in partners:
        if not p.get('partner_id'):
            errors.append(f"Missing partner_id: {p}")
        if not p.get('name'):
            errors.append(f"Missing name: {p}")
        if p.get('status') not in ['active', 'inactive']:
            errors.append(f"Invalid status for partner {p['partner_id']}: {p['status']}")
    
    if errors:
        # This will trigger a RETRY
        raise ValueError(f"Validation failed with {len(errors)} errors: {errors}")
    
    logger.info("All records passed validation!")
    context['ti'].xcom_push(key='validated_count', value=len(partners))


# ============================================================
# TASK 3: TRANSFORM
# Best Practice: pure function, same input = same output (idempotent)
# ============================================================
def transform_partners(**context):
    """
    IDEMPOTENCY:
    Transform is a pure function - no side effects.
    Running it 10 times with same input gives same output.
    """
    partners = context['ti'].xcom_pull(task_ids='extract_partners', key='raw_partners')
    ds = context['ds']
    
    logger.info(f"Transforming {len(partners)} partners for {ds}...")
    
    transformed = []
    for p in partners:
        if p['status'] == 'active':  # filter only active
            transformed.append({
                'partner_id': p['partner_id'],
                'name': p['name'].upper(),        # normalize name
                'country': p['country'],
                'processed_date': ds,             # partition by date
                'is_active': True
            })
    
    logger.info(f"Transformed: {len(transformed)} active partners kept from {len(partners)} total")
    context['ti'].xcom_push(key='transformed_partners', value=transformed)
    
    return len(transformed)


# ============================================================
# TASK 4: LOAD
# Best Practice: UPSERT instead of INSERT for idempotency
# ============================================================
def load_partners(**context):
    """
    IDEMPOTENCY IN LOADING:
    
    BAD (not idempotent - creates duplicates on re-run):
        INSERT INTO partners VALUES (...)
    
    GOOD (idempotent - safe to re-run):
        MERGE / UPSERT - update if exists, insert if not
        Or: DELETE partition first, then INSERT
    
    We simulate the idempotent pattern here:
    1. Delete existing records for this date
    2. Insert new records
    This means re-running gives the same result, no duplicates!
    """
    ds = context['ds']
    transformed = context['ti'].xcom_pull(task_ids='transform_partners', key='transformed_partners')
    
    logger.info(f"Loading {len(transformed)} records for partition: {ds}")
    logger.info(f"Step 1: DELETE FROM partners WHERE processed_date = '{ds}'")
    logger.info(f"Step 2: INSERT {len(transformed)} records")
    logger.info("Load complete - idempotent pattern applied!")
    
    # In real life with BigQuery:
    # bigquery_client.query(f"DELETE FROM `project.dataset.partners` WHERE processed_date = '{ds}'")
    # bigquery_client.insert_rows(table, transformed)


# ============================================================
# TASK 5: NOTIFY
# Best Practice: always have a final notification/audit task
# ============================================================
def notify_success(**context):
    ds = context['ds']
    validated_count = context['ti'].xcom_pull(task_ids='validate_partners', key='validated_count')
    transformed_count = context['ti'].xcom_pull(task_ids='transform_partners')
    
    logger.info(f"""
    ✅ Pipeline completed successfully!
    Date partition : {ds}
    Records extracted  : {validated_count}
    Records loaded     : {transformed_count}
    DAG run ID         : {context['run_id']}
    """)


# ============================================================
# DAG DEFINITION
# Best Practice: catchup=True for backfill, False for live
# ============================================================
with DAG(
    dag_id='partner_etl_advanced',
    default_args=default_args,
    description='Advanced Partner ETL with best practices',
    
    # SCHEDULE: run daily at midnight
    schedule_interval='@daily',
    
    # START DATE: when to start scheduling
    start_date=datetime(2024, 1, 1),
    
    # --------------------------------------------------------
    # CATCHUP / BACKFILL EXPLAINED:
    #
    # catchup=True  → Airflow will run ALL missed dates from
    #                 start_date until today when DAG is turned on.
    #                 Example: start_date=Jan1, turned on Mar1
    #                 → Airflow runs Jan1, Jan2, ... Feb28, Mar1
    #                 Use when: historical data must be processed
    #
    # catchup=False → Only run from today forward, skip past dates
    #                 Use when: only current data matters
    #
    # MANUAL BACKFILL via CLI:
    #   airflow dags backfill partner_etl_advanced \
    #     --start-date 2024-01-01 --end-date 2024-01-31
    # --------------------------------------------------------
    catchup=False,
    
    # Max concurrent runs (useful for backfill - don't overwhelm DB)
    max_active_runs=3,
    
    # Tags for filtering in UI
    tags=['partner', 'mdm', 'etl', 'advanced'],
    
    # Description shown in UI
    doc_md="""
    ## Partner ETL Advanced Pipeline
    
    Demonstrates Airflow best practices:
    - **Idempotency**: safe to re-run, no duplicates
    - **Retries**: automatic retry with exponential backoff
    - **XCom**: passing data between tasks
    - **Catchup/Backfill**: process historical data
    - **Validation**: check data before loading
    """
) as dag:

    # Dummy start task - best practice for complex DAGs
    start = EmptyOperator(task_id='start')

    task_extract = PythonOperator(
        task_id='extract_partners',
        python_callable=extract_partners,
        # Pass execution context (execution_date, ds, ti, etc.)
        provide_context=True,
    )

    task_validate = PythonOperator(
        task_id='validate_partners',
        python_callable=validate_partners,
        provide_context=True,
        # Override retries for this specific task
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    task_transform = PythonOperator(
        task_id='transform_partners',
        python_callable=transform_partners,
        provide_context=True,
    )

    task_load = PythonOperator(
        task_id='load_partners',
        python_callable=load_partners,
        provide_context=True,
        # Load is critical - more retries
        retries=5,
        retry_delay=timedelta(minutes=10),
    )

    task_notify = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success,
        provide_context=True,
        # Notification should not be retried
        retries=0,
    )

    # Dummy end task
    end = EmptyOperator(task_id='end')

    # --------------------------------------------------------
    # TASK DEPENDENCIES - define the pipeline order
    # --------------------------------------------------------
    start >> task_extract >> task_validate >> task_transform >> task_load >> task_notify >> end