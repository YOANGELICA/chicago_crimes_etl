from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from etl import read_csv, read_api_iucr, read_api_update, transform_csv,transform_update_data, transform_iucr, merge


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 14),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1) #It do the retries one minute before the first time, and it do it just one more time because the key "retries" said it to do it just once (the line before this one)
}


@dag(
    default_args=default_args,
    description='DAG for ETL project',
    schedule_interval='@daily',  
)

def etl_project():

    @task
    def extract_task_csv ():
        return read_csv()
    
    @task
    def extract_task_api_iucr ():
        return read_api_iucr()
    
    @task
    def extract_task_api_update ():
        return read_api_update()

    @task
    def transform_task_csv (json_data):
        return transform_csv(json_data)
    
    @task
    def transform_task_update_data (json_data):
        return transform_update_data(json_data)
    
    @task
    def transform_task_iucr(json_data):
        return transform_iucr(json_data)
    
    @task
    def merge_task(json_data, json_data2, json_data3):
        return merge(json_data, json_data2, json_data3)


    data_csv = extract_task_csv()
    data_api_iucr = extract_task_api_iucr ()
    data_api_update = extract_task_api_update ()

    data_tcsv = transform_task_csv(data_csv)
    data_tupdate = transform_task_update_data (data_api_update)
    data_tiucr= transform_task_iucr (data_api_iucr)


    merge_data = merge_task(data_tcsv, data_tupdate, data_tiucr)

    data_csv >> data_tcsv >> merge_data >> create_tables
    create_tables  >> [load_date, load_csv, load_iucr]
    data_api_update >> data_tupdate >> merge_data
    data_api_iucr >> data_tiucr >> merge_data



etl_project()