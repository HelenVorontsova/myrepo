# Partner ETL Pipeline

An Apache Airflow project that extracts, transforms, and loads partner master data.

## Project Structure

```
dags/
  partner_etl_dag.py          # Simple ETL DAG
  partner_etl_advanced.py     # Advanced ETL DAG with best practices
scripts/
  extract.py                  # Extract partner records
  transformy.py               # Filter and normalize partner data
  load.py                     # Load data into target system
docker-compose.yaml           # Airflow local environment
```

## DAGs

### `partner_etl_pipeline`
A straightforward daily ETL pipeline with three tasks:
- **extract_partners** — pulls partner records
- **transform_partners** — filters active partners, uppercases names
- **load_partners** — loads transformed records into the target system

Tasks share data via XCom.

### `partner_etl_advanced`
An extended version demonstrating Airflow best practices:
- **Idempotency** — uses `execution_date` for date partitioning, safe to re-run
- **Validation** — checks data quality before transforming
- **Retries** — exponential backoff per task
- **XCom** — data passed between all tasks
- **Catchup/Backfill** — configurable historical reprocessing
- **Audit** — notify task summarises each run

## Running Locally

Start Airflow with Docker Compose:

```bash
docker-compose up
```

Then open the Airflow UI at http://localhost:8080.
