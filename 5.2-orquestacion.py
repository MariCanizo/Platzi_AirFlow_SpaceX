#5.2-orquestacion

from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="5.2-orquestacion",
    description="Probando la orquestacion",
    schedule_interval="0 7 * * 1", # Los lunes a las 7 de la mañana
    start_date=datetime(2024, 4, 8),
    end_date=datetime(2024, 5, 8),
    #default_args={"depends_on_past":True},
    #max_active_runs=1
    ) as dag:

    t1 = EmptyOperator(task_id="tarea1")
    
    t2 = EmptyOperator(task_id="tarea2")
    
    t3 = EmptyOperator(task_id="tarea3")
    
    t4 = EmptyOperator(task_id="tarea4")
    
    t1 >> t2 >> t3 >> t4