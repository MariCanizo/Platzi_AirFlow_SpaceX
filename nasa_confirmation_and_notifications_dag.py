#nasa_confirmation_and_notifications_dag

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'nasa_confirmation_and_notifications',
    default_args=default_args,
    description='A DAG to simulate a confirmation from NASA, fetch data, and notify teams',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the bash command to simulate confirmation
confirmation_bash_command = 'sleep 20 && echo "OK" > /tmp/response_{{ ds_nodash }}.txt'

# Define the bash command to fetch data from NASA (SpaceX data in this case)
fetch_data_bash_command = "curl -o /tmp/history.json -L 'https://api.spacexdata.com/v4/history'"

# Define the Python function to simulate satellite answer
def _generate_platzi_data(**kwargs):
    data = pd.DataFrame({
        "student": ["Maria Cruz", "Daniel Crema", "Elon Musk", "Karol Castrejon", "Freddy Vega"],
        "timestamp": [kwargs['logical_date']] * 5
    })
    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv", header=True)

# Create the BashOperator task to simulate confirmation
simulate_confirmation_task = BashOperator(
    task_id='simulate_nasa_confirmation',
    bash_command=confirmation_bash_command,
    dag=dag,
)

# Create the BashOperator task to fetch data
fetch_data_task = BashOperator(
    task_id='fetch_nasa_data',
    bash_command=fetch_data_bash_command,
    dag=dag,
)

# Create the PythonOperator task to generate satellite data
generate_platzi_data_task = PythonOperator(
    task_id='generate_platzi_data',
    python_callable=_generate_platzi_data,
    provide_context=True,
    dag=dag,
)

# Create the EmailOperator task to notify the marketing team
notify_marketing_task = EmailOperator(
    task_id='notify_marketing_team',
    to='marketing@example.com',
    subject='Data is Available',
    html_content='The data is now available for the marketing team.',
    dag=dag,
)

# Create the EmailOperator task to notify the data analyst team
notify_data_analyst_task = EmailOperator(
    task_id='notify_data_analyst_team',
    to='data_analyst@example.com',
    subject='Data is Available',
    html_content='The data is now available for the data analyst team.',
    dag=dag,
)

# Set the task dependencies
simulate_confirmation_task >> fetch_data_task >> generate_platzi_data_task
generate_platzi_data_task >> notify_marketing_task
generate_platzi_data_task >> notify_data_analyst_task
