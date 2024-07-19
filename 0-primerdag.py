#0-primerdag.py

from airflow import DAG 
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="primerdag",
         description="Nuestro primer DAG",
         start_date=datetime(2024, 5, 8),
         schedule_interval="@once") as dag:
    
    t1 = EmptyOperator(task_id="dummy")
    t1
