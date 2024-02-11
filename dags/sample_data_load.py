from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataprocSubmitPySparkJobOperator, ClusterGenerator

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
project_id='nv-interview-chaitanya'
region='us-east4'
zone='us-east4-a'
gcp_conn_id='gcp_conn'

# Generate Dataproc cluster configuration with metadata
cluster_config = ClusterGenerator(
        project_id=project_id,
        num_workers=num_workers,
        region=region,
        zone=zone,
        image_version="2.2.5-debian12",  #desired image version here
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-4',
        master_disk_type='pd-standard',
        worker_disk_type='pd-standard',
        master_disk_size=100,
        worker_disk_size=50,
        properties={
            "spark.jars.packages": "io.delta:delta-core_2.12:2.4.0",
        },
        metadata={"GCS_CONNECTOR_VERSION": "2.2.2","gcs-connector-version":"2.2.2"}  #  metadata here
    ).make()

create_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name=cluster_name,
    cluster_config=cluster_config,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)
load_data ='sample_data_gen'
generate_sample_data = DataprocSubmitPySparkJobOperator(
    task_id = load_data,
    main= 'gs://nv-interview-chaitanya/sample_data_gen_using_spark.py',
    cluster_name=cluster_name,
    region=region,
    project_id=project_id,
    dataproc_properties={
            "spark.jars.packages": "io.delta:delta-core_2.12:2.4.0",
        },
    job_name=load_data,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

delete_cluster = DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    cluster_name=cluster_name,
    project_id=project_id,
    region=region,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

create_cluster >> generate_sample_data >> delete_cluster
