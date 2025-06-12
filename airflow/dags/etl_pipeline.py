from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
import os

# Assuming your python scripts are in the scripts/ directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts')))
from extract_website1 import extract_website1_data
from extract_website2 import extract_website2_data
from load_to_postgres import load_data_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Extract data from websites, load to Postgres, clean with dbt',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 1. Extract data from Website 1
    extract_website1 = PythonOperator(
        task_id='extract_website1',
        python_callable=extract_website1_data,
        dag=dag,
    )

    # 2. Extract data from Website 2
    extract_website2 = PythonOperator(
        task_id='extract_website2',
        python_callable=extract_website2_data,
        dag=dag,
    )

    # 3. Load extracted data (CSV files) into Postgres
    load_to_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_data_to_postgres,
        dag=dag,
    )

    # 4. Run dbt to clean and transform data in Postgres
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /usr/local/airflow/dbt && dbt run',
        dag=dag,
    )

    # Task dependencies
    [extract_website1, extract_website2] >> load_to_postgres >> dbt_run