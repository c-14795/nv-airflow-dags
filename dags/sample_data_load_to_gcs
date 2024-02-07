from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import gcsfs

gcs_bucket = 'nv-interview-chaitanya'
gcs_project_id = 'nv-interview-chaitanya'
# Define the data as a dictionary
users_data = {
    'id': [1, 2],
    'username': ['kat@email.com', 'mani@email.com'],
    'metadata': [
        '{"secret": "gQTKNMafpw", "provider": "google-oauth2"}',
        '{"secret": "sjmaIS2EmA", "provider": "basic-auth"}'
    ],
    'created_at': ['2023-09-01 08:01:02', '2023-09-01 08:01:03']
}
groups_data = {
    'id': [1, 2],
    'name': ['SUPER_USER', 'DEFAULT_USER'],
    'description': ['Full access to all functions.', 'Initial role assigned to a new user.'],
    'created_at': ['2015-01-01 04:05:06', '2015-01-01 05:06:07']
}

def get_users_df():
    # Create a DataFrame from the data
    df = pd.DataFrame(users_data)
    
    # Store metadata as string
    df['metadata'] = df['metadata'].astype(str)
    
    # Convert created_at column to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df

def get_groups_df():
    # Create a DataFrame from the data
    df = pd.DataFrame(groups_data)
    
    # Store metadata as string
    df['metadata'] = df['metadata'].astype(str)
    
    # Convert created_at column to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df

# Function to write DataFrame to GCS
def write_pandas_df_to_gcs(gcs_path,df):
    # Write DataFrame to GCS
    with gcsfs.GCSFileSystem(project=gcs_project_id).open(gcs_path, 'wb') as f:
        df.to_parquet(f, index=False, engine='pyarrow')

# Function to write users data to gcs
def write_users_to_gcs():
    # Specify GCS bucket and filename
    users_filename = f'{gcs_bucket}/interview_data/authentication_users/users.parquet'
    df = get_users_df()
    write_pandas_df_to_gcs(users_filename,df)

# Function to write groups data to gcs
def write_groups_to_gcs():
    # Specify GCS bucket and filename
    groups_filename = f'{gcs_bucket}/interview_data/authentication_groups/groups.parquet'
    df = get_groups_df()
    write_pandas_df_to_gcs(groups_filename,df)
    
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG, can be done using context manager with as well.
dag = DAG(
    'write_to_gcs',
    default_args=default_args,
    description='Write DataFrame to GCS',
    schedule_interval=None,
)

# Define the PythonOperator
write_users_to_gcs_task = PythonOperator(
    task_id='write_users_to_gcs',
    python_callable=write_users_to_gcs,
    dag=dag,
)
write_groups_to_gcs_task = PythonOperator(
    task_id='write_groups_to_gcs',
    python_callable=write_groups_to_gcs,
    dag=dag,
)

write_users_to_gcs >> write_groups_to_gcs
