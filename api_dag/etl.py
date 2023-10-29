import requests
import pandas as pd
import json
import logging
from sodapy import Socrata
import api_dag.transform
import api_dag.db_queries as db_queries

def read_csv():
    df = pd.read_csv("./data/Crimes_2001_to_Present.csv")
    logging.info("MY DF: ", df)
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
    return df.to_json(orient='records')

def transform_csv(json_data):
    logging.info("MY JSON DATA IS: ", json_data)
    logging.info("TYPE OF JSON DATA: ", type(json_data))

    data = json_data
    data = json.loads(data)
    df=pd.DataFrame(data)
    logging.info("MY DATAFRAME", df)

    df=api_dag.transform.split_datetime(df)
    df=api_dag.transform.move_time(df)
    df=api_dag.transform.change_updated_on_format(df)
    df=api_dag.transform.convert_dtype(df)
    df=api_dag.transform.replace_nulls(df)
    df=api_dag.transform.change_dtype_columns(df)
    df=api_dag.transform.change_columns_names(df)
    df=api_dag.transform.create_point(df)
    df=api_dag.transform.drop_na_location(df)
    df=api_dag.transform.drop_columns(df)

    return df.to_json(orient='records')

def transform_update_data(json_data):
    logging.info("MY JSON DATA IS: ", json_data)
    logging.info("TYPE OF JSON DATA: ", type(json_data))

    data = json_data
    data = json.loads(data)
    df=pd.DataFrame(data)
    logging.info("MY DATAFRAME", df)

    df=api_dag.transform.drop_columns_newdata(df)
    df=api_dag.transform.create_point(df)
    df=api_dag.transform.split_datetime_newdata(df)
    df=api_dag.transform.replace_nulls_newdata(df)
    df=api_dag.transform.change_columns_dtype_newdata(df)
    df=api_dag.transform.move_time(df)
    df=api_dag.transform.change_columns_names(df)
    df=api_dag.transform.drop_na_location(df)
    df=api_dag.transform.drop_columns(df)
    df=api_dag.transform.drop_more_columns_newdata(df)

    return df.to_json(orient='records')

def transform_iucr(json_data):
    logging.info("MY JSON DATA IS: ", json_data)
    logging.info("TYPE OF JSON DATA: ", type(json_data))

    data = json_data
    data = json.loads(data)
    df=pd.DataFrame(data)
    logging.info("MY DATAFRAME", df)

    df['iucr'] = df['iucr'].apply(lambda x: '0' + x if len(x) == 3 else x)

    return df.to_json(orient='records')

#TODO: Queda pendiente el borrar los iucr que no usa la tabla de crimes

def merge(json_data, json_data2, json_data3):
    pass

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

def load_crimes(json_data):
    logging.info("MY JSON DATA IS: ", json_data)
    logging.info("TYPE OF JSON DATA: ", type(json_data))

    data = json_data
    data = json.loads(data)
    df=pd.DataFrame(data)
    logging.info("MY DATAFRAME", df)


    db_queries.insert_info_crimes(df)

def load_iucr(json_data):
    logging.info("MY JSON DATA IS: ", json_data)
    logging.info("TYPE OF JSON DATA: ", type(json_data))

    data = json_data
    data = json.loads(data)
    df=pd.DataFrame(data)
    logging.info("MY DATAFRAME", df)

    db_queries.insert_info_iucr()

def load_date(json_data):
    pass


