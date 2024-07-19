#5.3-orquestacion

from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="5.3-orquestacion",
    description="Probando la orquestacion",
    schedule_interval="@monthly", # Si el mes NO terminó, no se ejecuta, Airflow ejecuta en intérvalos
    start_date=datetime(2024, 1, 8),
    end_date=datetime(2024, 5, 8),
    #default_args={"depends_on_past":True},
    #max_active_runs=1
    ) as dag:

    t1 = EmptyOperator(task_id="tarea1")
    
    t2 = EmptyOperator(task_id="tarea2")
    
    t3 = EmptyOperator(task_id="tarea3")
    
    t4 = EmptyOperator(task_id="tarea4")
    
    t1 >> t2 >> t3 >> t4