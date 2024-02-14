from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator
)
from kubernetes import client, config
import json
from airflow.models import Variable
import os
import yaml

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

def fetch_config_maps():
    try:
        # Load kube config from default location or provide the path to the kube config file
        config.load_kube_config()

        # Create a Kubernetes API client
        api_instance = client.CoreV1Api()

        # Fetch all ConfigMaps in the airflow namespace
        namespace = 'airflow-ns'
        config_maps = api_instance.list_namespaced_config_map(namespace)

        # Convert ConfigMaps to a dictionary for JSON serialization
        config_maps_dict = {cm.metadata.name: cm.data for cm in config_maps.items}

        # Print or process the fetched ConfigMaps
        print("ConfigMaps in the airflow namespace:")
        print(json.dumps(config_maps_dict, indent=4))

    except Exception as e:
        print("Exception:", e)

def read_config_maps():
    try:
        if not os.path.exists("/opt/airflow/config/pii-config/data"):
            print("file not found!! please check")
        else:
            with open("/opt/airflow/config/pii-config/data", "r") as stream:
                data = yaml.safe_load(stream)
                print(data)
    except Exception as e:
        print("exception")
        print(e)
                

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
    print(Variable.get("pii_map"))
    

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2_function,
    dag=dag,
)
task3 = PythonOperator(
    task_id='task3',
    python_callable=read_config_maps,
    dag=dag,
)
end_task = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start_task >>[ task1, task2, task3] >> end_task
