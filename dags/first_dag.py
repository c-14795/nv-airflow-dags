from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator
)

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

pod_task_xcom = GKEStartPodOperator(
    task_id="pod_task_xcom",
    project_id="nv-interview-chaitanya",
    location="us-east4",
    cluster_name="nv-east",
    do_xcom_push=True,
    namespace="airflow-ns",
    image="alpine",
    cmds=["bash", "-cx"],
    arguments=["kubectl get configmaps -n airflow-ns"],
    name="test-pod-xcom",
    in_cluster=True,
    on_finish_action="delete_pod",
)
end_task = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start_task >>[ task1, task2, pod_task_xcom] >> end_task
