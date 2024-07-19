from airflow import DAG
from datetime import datetime
from HelloOperator import HelloOperator


with DAG(
    dag_id="customeoperator",
    description="Nuestro primer custome operator",
    start_date=datetime(2024, 5, 8)) as dag:

    t1 = HelloOperator(
        task_id="hello",
        name="Marina"
    )