from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG object
dag = DAG(
    'basic_dag',
    default_args=default_args,
    description='A basic DAG example',
    schedule_interval=None,  # No fixed schedule
)

# Define tasks
start_task = DummyOperator(task_id='start', dag=dag)

def task1_function():
    print("Executing Task 1")

task1 = PythonOperator(
    task_id='task1',
    python_callable=task1_function,
    dag=dag,
)

def task2_function():
    print("Executing Task 2")

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2_function,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start_task >> task1 >> task2 >> end_task
