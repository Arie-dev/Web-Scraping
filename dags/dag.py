from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from Step1_Books_Bronze import booksBronzeLayer
from Step1_Quotes_Bronze import quotesBronzeLayer
from Step2_Cleaning_Silver import process_silver_layer
from Step3_Enrich_Gold import process_gold_layer

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dataIncubatorDag',
    default_args=default_args,
    description='This dag runs our data incubator project',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 20),
    catchup=False
):
    task1_0 = PythonOperator(
        task_id='booksBronzeLayer',
        python_callable=booksBronzeLayer,
    )
    task1_1 = PythonOperator(
        task_id='quotesBronzeLayer',
        python_callable=quotesBronzeLayer,
    )
    task2 = PythonOperator(
        task_id='processSilverLayer',
        python_callable=process_silver_layer,
    )
    task3 = PythonOperator(
        task_id='processGoldLayer',
        python_callable=process_gold_layer,
    )

    [task1_0, task1_1] >> task2 >> task3
