from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="5.1-orquestacion",
    description="Probando la orquestacion",
    schedule_interval="@daily", 
    start_date=datetime(2024, 4, 8),
    end_date=datetime(2024, 5, 8),
    default_args={"depends_on_past":True}, #si lo programo en TRUE, significa que la tarea siguiente, depende de la anterior
    max_active_runs=1) as dag: # Siginifica que UN DÍA se ejecuta a la vez 

    t1 = BashOperator(
        task_id="tarea1",
        bash_command="sleep 2 && echo 'Tarea1'")
    
    t2 = BashOperator(
        task_id="tarea2",
        bash_command="sleep 2 && echo 'Tarea2'")
    
    t3 = BashOperator(
        task_id="tarea3",
        bash_command="sleep 2 && echo 'Tarea3'")
    
    t4 = BashOperator(
        task_id="tarea4",
        bash_command="sleep 2 && echo 'Tarea4'")
    
    t1 >> t2 >> [t3, t4]