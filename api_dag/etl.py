import requests
import pandas as pd
import json
import logging
from sodapy import Socrata
import transform
import db_queries

def read_csv():
    df = pd.read_csv("./data/filtered_data.csv")
    logging.info("MY DF: ", df)
    logging.info("df shape: ",df)
    return df.to_json(orient='records')

def read_api_iucr():
    url = "https://data.cityofchicago.org/resource/c7ck-438e.json"
    try:
        response = requests.get(url)
        data = response.json()
        iucr = [x['iucr'] for x in data]
        primary_description = [x['primary_description'] for x in data]
        secondary_description = [x['secondary_description'] for x in data]
        secondary_description = [x['secondary_description'] for x in data]
        index_code = [x['index_code'] for x in data]
        active = [x['active'] for x in data]
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

    data = {
                'iucr': iucr,
                'primary_description': primary_description,
                'secondary_description':secondary_description,
                'index_code': index_code,
                'active': active
            }

    df = pd.DataFrame(data)
    logging.info ("MY DF:", df.head())
    logging.info ("MY DF SHAPE:", df.shape)
    return df.to_json(orient='records')

def read_api_update():

    df = pd.read_csv("./data/new_data.csv")
    logging.info("MY DF: ", df)
    logging.info("df shape: ",df)
    """
    with open('./secrets/api_credentials.json') as config_json:
        config = json.load(config_json)
        socrata_domain = config["socrata_domain"]
        socrata_token = config["socrata_token"]
        socrata_dataset_identifier = config["socrata_dataset_identifier"]

    client = Socrata(socrata_domain, socrata_token, timeout=100)

    offset = 7000000
    batch_size = 1000

    all_records = []

    while True:

        results = client.get(socrata_dataset_identifier, limit=batch_size, offset=offset)
        if not results:
            break
        all_records.extend(results)
        offset += batch_size

    logging.info("Total records retrieved:", len(all_records))

    df = pd.DataFrame(all_records)
    df.to_csv("./data/new_data.csv", index=False)
   """
    return df.to_json(orient='records')

def transform_csv(**kwargs):
    logging.info("kwargs are: ", kwargs.keys())

    ti = kwargs['ti']
    logging.info("ti: ",ti)

    str_data = ti.xcom_pull(task_ids="read_csv_task")
    logging.info(f"str_data: {str_data}")

    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataframe intial shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    df=transform.split_datetime(df)
    df=transform.move_time(df)
    df=transform.change_updated_on_format(df)
    df=transform.convert_dtype(df)
    df=transform.replace_nulls(df)
    df=transform.change_dtype_columns(df)
    df=transform.change_columns_names(df)
    df=transform.create_point(df)
    df=transform.drop_na_location(df)
    df=transform.drop_columns(df)

    return df.to_json(orient='records')

def transform_update_data(**kwargs):
    logging.info("kwargs are: ", kwargs.keys())

    ti = kwargs['ti']
    logging.info("ti: ",ti)

    str_data = ti.xcom_pull(task_ids="read_update_task")
    logging.info(f"str_data: {str_data}")

    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataframe intial shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    df=transform.drop_columns_newdata(df)
    df=transform.create_point(df)
    df=transform.split_datetime_newdata(df)
    df=transform.replace_nulls_newdata(df)
    df=transform.change_columns_dtype_newdata(df)
    df=transform.move_time(df)
    df=transform.change_columns_names(df)
    df=transform.drop_na_location(df)
    df=transform.drop_columns(df)

    return df.to_json(orient='records')

def transform_iucr(**kwargs):
    logging.info("kwargs are: ", kwargs.keys())

    ti = kwargs['ti']
    logging.info("ti: ",ti)

    str_data = ti.xcom_pull(task_ids="read_iucr_task")
    logging.info(f"str_data: {str_data}")

    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataframe intial shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    df['iucr'] = df['iucr'].apply(lambda x: '0' + x if len(x) == 3 else x)

    return df.to_json(orient='records')

#TODO: Queda pendiente el borrar los iucr que no usa la tabla de crimes

def merge(**kwargs):
    logging.info("kwargs are: ", kwargs.keys())

    ti = kwargs['ti']
    logging.info("ti: ",ti)

    str_data = ti.xcom_pull(task_ids= ["transform_csv_task","transform_update_task"])
    csv_data_str = str_data[0]
    update_data_str = str_data[1]

    # Updated data (transformed):   
    logging.info(f"Updated data: {update_data_str}")
    json_data_updated = json.loads(update_data_str)
    update_df = pd.json_normalize(data=json_data_updated)
    logging.info(f"FIRST DF - UPDATED INFO: {update_df.shape}")

    # Data from the csv file (transformed):
    logging.info(f"Csv data: {csv_data_str}")
    json_data_csv = json.loads(csv_data_str)
    csv_df = pd.json_normalize(data=json_data_csv)
    logging.info(f"SECOND DF - CSV INFO: {csv_df.shape}")

    df = pd.concat([csv_df, update_df], ignore_index=True)

    return df.to_json(orient='records')

def create_date(**kwargs):

    logging.info("kwargs are: ", kwargs.keys())

    ti = kwargs['ti']
    logging.info("ti: ",ti)

    str_data = ti.xcom_pull(task_ids="merge_task")
    logging.info(f"str_data: {str_data}")

    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    date_df = df[['date']]
    date_df = date_df.drop_duplicates().reset_index(drop=True)

    logging.info(f"date df shape: {date_df.shape}") # should be (8299, 1)
    
    date_df['date'] = pd.to_datetime(date_df['date']) # EN CAASO DE QUE NO RECIBA LA COL DATE COMO DATETIME, SI SÍ LA RECIBE, BORRAR ESTO AKSBFHAS
    date_df['date_id'] = date_df['date'].dt.strftime('%Y%m%d')
    date_df['year'] = date_df['date'].dt.year
    date_df['month'] = date_df['date'].dt.strftime('%B')
    date_df['day_week'] = date_df['date'].dt.strftime('%A')

    logging.info(f'first row: {date_df.iloc[0].values}')

    return date_df.to_json(orient='records')


def create_tables():

    db_queries.create_table_crimes()

    description_crimes= db_queries.describe_crimes()
    desc_crimes=pd.DataFrame(description_crimes, columns=['Field', 'Type', 'Null', 'Key', 'Default', 'Extra'])
    logging.info(desc_crimes)

    ###
    db_queries.create_table_iucr()

    description_iucr= db_queries.describe_iucr()
    desc_iucr=pd.DataFrame(description_iucr, columns=['Field', 'Type', 'Null', 'Key', 'Default', 'Extra'])
    logging.info(desc_iucr)

    ###
    db_queries.create_table_dates()

    description_dates= db_queries.describe_dates()
    desc_dates=pd.DataFrame(description_dates, columns=['Field', 'Type', 'Null', 'Key', 'Default', 'Extra'])
    logging.info(desc_dates)

def load_crimes(**kwargs):
    logging.info("kwargs are: ", kwargs.keys())

    ti = kwargs['ti']
    logging.info("ti: ",ti)

    str_data = ti.xcom_pull(task_ids="merge_task")
    logging.info(f"str_data: {str_data}")

    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataframe intial shape: {df.shape[0]} Rows and {df.shape[1]} Columns")


    db_queries.insert_info_crimes(df)

def load_iucr(**kwargs):
    logging.info("kwargs are: ", kwargs.keys())

    ti = kwargs['ti']
    logging.info("ti: ",ti)

    str_data = ti.xcom_pull(task_ids="transform_iucr_task")
    logging.info(f"str_data: {str_data}")

    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataframe intial shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    db_queries.insert_info_iucr()

def load_date(**kwargs):
    logging.info("kwargs are: ", kwargs.keys())

    ti = kwargs['ti']
    logging.info("ti: ",ti)

    str_data = ti.xcom_pull(task_ids="create_date_task")
    logging.info(f"str_data: {str_data}")

    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataframe intial shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    db_queries.insert_info_dates()


