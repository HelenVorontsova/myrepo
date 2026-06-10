from airflow.decorators import dag, task
from datetime import datetime

@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["mine"],
)
def my_first_dag():

    @task
    def say_hello():
        print("Hello from my DAG!")

    say_hello()

my_first_dag()
