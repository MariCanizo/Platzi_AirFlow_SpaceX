from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="bashoperator",
    description="Utilizando bach operator",
    start_date=datetime(2024, 5, 8)) as dag:

    t1 = BashOperator(task_id="hello_with_bash",
                      bash_command="echo 'Hello gente de Platzi'")
    t1