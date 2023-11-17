from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from etl import read_csv, read_api_iucr, read_api_update, transform_csv, transform_update_data, transform_iucr, merge, create_date, create_tables, load_crimes, load_iucr, load_date, kafka_producer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'etl_project',
    default_args=default_args,
    description='ETL project DAG',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv,
        provide_context = True,
    )

    read_iucr_task = PythonOperator(
        task_id='read_iucr_task',
        python_callable=read_api_iucr,
        provide_context = True,
        )

    read_update_task = PythonOperator(
        task_id='read_update_task',
        python_callable=read_api_update,
        provide_context = True,
        )

    transform_csv_task = PythonOperator(
        task_id='transform_csv_task',
        python_callable=transform_csv,
        provide_context = True,
        )
    
    transform_update_task = PythonOperator(
        task_id='transform_update_task',
        python_callable=transform_update_data,
        provide_context = True,
        )
    
    transform_iucr_task = PythonOperator(
        task_id='transform_iucr_task',
        python_callable=transform_iucr,
        provide_context = True,
        )
    
    merge_task = PythonOperator(
        task_id='merge_task',
        python_callable = merge,
        provide_context = True,
        )
    
    create_date_task = PythonOperator(
    task_id = 'create_date_task',
    python_callable = create_date,
    provide_context = True
    )

    create_tables_task = PythonOperator(
        task_id = 'create_tables_task',
        python_callable = create_tables,
        provide_context = True,
        )

    load_crimes_task = PythonOperator(
        task_id = 'load_crimes_task',
        python_callable = load_crimes,
        provide_context = True
        )

    load_iucr_task = PythonOperator(
        task_id = 'load_iucr_task',
        python_callable = load_iucr,
        provide_context = True
        )

    load_date_task = PythonOperator(
        task_id = 'load_date_task',
        python_callable = load_date,
        provide_context = True
        )

    kafka_producer_task = PythonOperator(
	task_id= 'kafka_producer_task',
	python_callable = kafka_producer,
	provide_context = True
	)

    read_csv_task >> transform_csv_task >> merge_task >> create_date_task >> create_tables_task
    create_tables_task >> [load_crimes_task, load_iucr_task, load_date_task] >> kafka_producer_task
    read_iucr_task >> transform_iucr_task >> merge_task 
    read_update_task >> transform_update_task >> merge_task
