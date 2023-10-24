import requests
import pandas as pd
import json
import logging
import psycopg2 as psy
from sodapy import Socrata
import transformations.transform

def create_db_connection():
    with open('./secrets/config_db.json') as config_json:
        config = json.load(config_json)
    conx = psy.connect(**config) 
    return conx

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

    df=transformations.transform.split_datetime(df)
    df=transformations.transform.move_time(df)
    df=transformations.transform.change_updated_on_format(df)
    df=transformations.transform.convert_dtype(df)
    df=transformations.transform.replace_nulls(df)
    df=transformations.transform.change_dtype_columns(df)
    df=transformations.transform.change_columns_names(df)
    df=transformations.transform.create_point(df)
    df=transformations.transform.drop_na_location(df)
    df=transformations.transform.drop_columns(df)

    return df.to_json(orient='records')

def transform_update_data(json_data):
    logging.info("MY JSON DATA IS: ", json_data)
    logging.info("TYPE OF JSON DATA: ", type(json_data))

    data = json_data
    data = json.loads(data)
    df=pd.DataFrame(data)
    logging.info("MY DATAFRAME", df)

    df=transformations.transform.drop_columns_newdata(df)
    df=transformations.transform.create_point(df)
    df=transformations.transform.split_datetime_newdata(df)
    df=transformations.transform.replace_nulls_newdata(df)
    df=transformations.transform.change_columns_dtype_newdata(df)
    df=transformations.transform.move_time(df)
    df=transformations.transform.change_columns_names(df)
    df=transformations.transform.drop_na_location(df)
    df=transformations.transform.drop_columns(df)
    df=transformations.transform.drop_more_columns_newdata(df)

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
    



if __name__ == '__main__':

