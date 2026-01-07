# airflow/dags/custom_dbt_dag.py
from airflow import DAG
from datetime import timedelta
from datetime import datetime
import os

# Import our custom operator
from dbt_operator import DbtOperator

# Get environment variables
ANALYTICS_DB = os.getenv('ANALYTICS_DB', 'analytics')
PROJECT_DIR = os.getenv('AIRFLOW_HOME')+"/dags/dbt/my_dbt_project"
PROFILE = 'my_dbt_project'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'custom_dbt_transformations',
    default_args=default_args,
    description='Run dbt transformations with custom operator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025,4,22),
    catchup=False,
    tags=['dbt', 'custom'],
)

# Environment variables to pass to dbt
env_vars = {
    'ANALYTICS_DB': ANALYTICS_DB,
    'DBT_PROFILE': PROFILE
}

# Example of variables to pass to dbt
dbt_vars = {
    'is_test': False,
    'data_date': '{{ ds }}',  # Uses Airflow's ds (execution date) macro
}

# Step 1: Run dbt run to execute models
dbt_run = DbtOperator(
    task_id='dbt_run',
    dag=dag,
    command='run',
    profile=PROFILE,
    project_dir=PROJECT_DIR,
    # Example of selecting specific models
    models=['example'],  # This selects all staging model
    env_vars=env_vars,
    vars=dbt_vars,
)

# Step 2: Run dbt test to validate results
dbt_test = DbtOperator(
    task_id='dbt_test',
    dag=dag,
    command='test',
    profile=PROFILE,
    project_dir=PROJECT_DIR,
    # Example of using custom operator parameters
    fail_fast=True,
    env_vars=env_vars,
    vars=dbt_vars,
)
# Define the task dependencies
dbt_run >> dbt_test 