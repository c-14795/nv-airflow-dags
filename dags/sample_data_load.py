from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitPySparkJobOperator, ClusterGenerator
from airflow.models import Variable

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
gcp_conn_id='gcp_con'

# reads data from pii map
def read_config_maps(**context):
    try:
        if not os.path.exists("/opt/airflow/config/pii-config/data"):
            print("file not found!! please check")
        else:
            with open(os.path.join(Variable.get("pii-map"),'data'), "r") as stream:
                data = yaml.safe_load(stream)
                print(data) # for testing purposes
                context['ti'].xcom_push(key='pii-map', value=json.dumps(data))
                
    except Exception as e:
        print("exception")
        print(e)
    

# Generate Dataproc cluster configuration with metadata
## This can be wrapped around a function and re-utilised, keeping it simple for scope for assignment
cluster_config = ClusterGenerator(
        project_id=project_id,
        num_workers=2,
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
            "spark:spark.jars.packages": "io.delta:delta-spark_2.12:3.1.0",
        },
        metadata={"GCS_CONNECTOR_VERSION": "2.2.2","gcs-connector-version":"2.2.2"}  #  metadata here
    ).make()

create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=cluster_name,
    cluster_config=cluster_config,
    gcp_conn_id=gcp_conn_id,
    region=region,
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
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.1.0",
        },
    job_name=load_data,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)
pii_map_fetcher = PythonOperator(
    task_id='pii_map_fetcher',
    python_callable=read_config_maps,
    provide_context=True
    dag=dag,
)
load_masked_pii = 'mask_pii'
mask_pii = DataprocSubmitPySparkJobOperator(
    task_id = load_masked_pii,
    main= 'gs://nv-interview-chaitanya/mask_pii.py',
    cluster_name=cluster_name,
    region=region,
    project_id=project_id,
    dataproc_properties={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.1.0",
        },
    job_name=load_masked_pii,
    gcp_conn_id=gcp_conn_id,
    dag=dag,
    arguments=["--pii_map","{{ task_instance.xcom_pull(task_ids='pii_map_fetcher',key='pii-map') }}"]
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    cluster_name=cluster_name,
    project_id=project_id,
    region=region,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

[pii_map_fetcher, create_cluster] >> generate_sample_data >> mask_pii >> delete_cluster
