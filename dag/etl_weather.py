import pendulum
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

from extract_data import extract
from load_data import load
from transform_data import transform
from airflow.operators.bash import BashOperator
with DAG(
        "etl_weather_data_sync",
        default_args={"retries": 2},
        description="DAG tutorial",
        schedule_interval='*/15 * * * *',
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["etl"],
) as dag:

    def extract_w_data(**kwargs):
        extract()


    def transform_w_data(**kwargs):
        transform()


    def load_w_data(**kwargs):
        load()


    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_w_data,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_w_data,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_w_data,
    )
# BashOperator to create the directory
    create_extract_folder = BashOperator(
        task_id='create_extract',
        bash_command='mkdir -p /home/astro/etl_storage/extract',
        dag=dag
    )

    create_transform_folder = BashOperator(
        task_id='create_transform',
        bash_command='mkdir -p /home/astro/etl_storage/transform',
        dag=dag
    )
    create_extract_folder >> create_transform_folder >> extract_task >> transform_task >> load_task
