from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('create_dataproc_single_master_cluster', default_args=default_args, schedule_interval=None)

cluster_name = "single-master-cluster"

create_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name=cluster_name,
    project_id='nv-interview-chaitanya',
    num_workers=0,
    num_masters=1,
    zone='us-east4-a',
    master_machine_type='n1-standard-2',
    dag=dag
)

delete_cluster = DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    cluster_name=cluster_name,
    project_id='nv-interview-chaitanya',
    dag=dag
)

create_cluster >> delete_cluster
