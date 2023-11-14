from datetime import datetime
import json
import logging
import os
import pandas as pd
import psycopg2 as psy


def create_db_connection():

    script_dir = os.path.dirname(__file__)
    json_file = os.path.join(script_dir, '../secrets/config_db.json')

    with open(json_file) as config_json:
        config = json.load(config_json)

    try:
        conx = psy.connect(**config) 
        print("Successful Connection")
    
    except psy.Error as e:
        conx = None
        print(f"Connection failed {e}")

    return conx

#### CRIMES
def create_table_crimes():
    try:
        conx = create_db_connection()
        mycursor = conx.cursor()

        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS crimes (
            id SERIAL PRIMARY KEY,
            date DATE,
            time TIME,
            block VARCHAR(150),
            iucr VARCHAR(10),
            location_desc VARCHAR(150),
            arrest BOOLEAN,
            district INT,
            year INT,
            latitude float,
            longitude float
        );
    """)

        conx.commit()
        mycursor.close()
        conx.close()
        return "ok"
    
    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"

def describe_crimes():
    try:
        conx = create_db_connection()
        mycursor = conx.cursor()
        mycursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'crimes';")

        description_crimes = mycursor.fetchall()
        return description_crimes
    
    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"

def insert_info_crimes(df):
    try:
        conx = create_db_connection()
        mycursor = conx.cursor()

        for _, i in df.iterrows():
        #        date_value = datetime.fromtimestamp(i['date'] / 1000).date()
            arrest_value = bool(i['arrest'])

            insert = """INSERT INTO crimes 
                        (id, date, time, block, iucr, location_desc, arrest, district, year, latitude, longitude) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) 
                        DO UPDATE SET 
                            date = EXCLUDED.date, 
                            time = EXCLUDED.time, 
                            block = EXCLUDED.block,
                            iucr = EXCLUDED.iucr,
                            location_desc = EXCLUDED.location_desc,
                            arrest = EXCLUDED.arrest,
                            district = EXCLUDED.district,
                            year = EXCLUDED.year,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude;"""

            datos = (
                i['id'],
                i['date'],
                i['time'],
                i['block'],
                i['iucr'],
                i['location_desc'],
                arrest_value,
                i['district'],
                i['year'],
                i['latitude'],
                i['longitude'],
            )

            mycursor.execute(insert, datos)

        conx.commit()
        mycursor.close()
        conx.close()
        return "ok"

    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"

#### IUCR
def create_table_iucr():
    try:
        conx = create_db_connection()
        mycursor = conx.cursor()

        mycursor.execute("""CREATE TABLE IF NOT EXISTS codes (
                            iucr VARCHAR(10) PRIMARY KEY,
                            primary_description VARCHAR(50),
                            secondary_description VARCHAR(70),
                            index_code VARCHAR(5),
                            active BOOLEAN
                            );""")

        conx.commit()
        mycursor.close()
        conx.close()
        return "ok"
    
    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"     

def describe_iucr():
    try:
        conx = create_db_connection()
        mycursor = conx.cursor()

        mycursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'codes';")


        description_iucr = mycursor.fetchall()

        mycursor.close()
        return description_iucr
    
    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"

def insert_info_iucr(df):

    try:
        conx = create_db_connection()
        mycursor = conx.cursor()
        
        for _, i in df.iterrows():
            insert = """INSERT INTO codes 
                        (iucr, primary_description, secondary_description, index_code, active) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (iucr) 
                        DO UPDATE SET 
                            iucr = EXCLUDED.iucr, 
                            primary_description = EXCLUDED.primary_description, 
                            secondary_description = EXCLUDED.secondary_description,
                            index_code = EXCLUDED.index_code,
                            active = EXCLUDED.active;
                        """

            datos = (
                i['iucr'],
                i['primary_description'], 
                i['secondary_description'], 
                i['index_code'], 
                i['active']
            )

            mycursor.execute(insert, datos)
        conx.commit()
        mycursor.close()
        conx.close()
        return "ok"

    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"

#### DATES

def create_table_dates():
    try:

        conx = create_db_connection()
        mycursor = conx.cursor()

        mycursor.execute("""CREATE TABLE IF NOT EXISTS dates (
                            date_id VARCHAR(10) PRIMARY KEY,
                            date DATE,
                            year INT,
                            month VARCHAR(10),
                            day_week VARCHAR(10)
                            );""")

        conx.commit()
        mycursor.close()
        conx.close()
        return "ok"
    
    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"

def describe_dates():
    try: 
        conx = create_db_connection()
        mycursor = conx.cursor()

        mycursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dates';")


        description_iucr = mycursor.fetchall()

        mycursor.close()
        return description_iucr
    
    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"

def insert_info_dates(df):
    try:
        conx = create_db_connection()
        mycursor = conx.cursor()
        
        for _, i in df.iterrows():
    #        date_value = datetime.utcfromtimestamp(i['date']).strftime('%Y-%m-%d %H:%M:%S')

            insert = """INSERT INTO dates 
                        (date_id, date, year, month, day_week) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (date_id) 
                        DO UPDATE SET 
                            date_id = EXCLUDED.date_id, 
                            date = EXCLUDED.date, 
                            year = EXCLUDED.year,
                            month = EXCLUDED.month,
                            day_week = EXCLUDED.day_week;
                        """

            datos = (
                i['date_id'], 
                i['date'],
                i['year'], 
                i['month'], 
                i['day_week']
            )

            mycursor.execute(insert, datos)
        conx.commit()
        mycursor.close()
        conx.close()
        return "ok"
    
    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return "error"


def get_crimes_data():
        
    try: 
        conx = create_db_connection()
        cursor = conx.cursor()

        get_data = "SELECT * FROM crimes"
        
        cursor.execute(get_data)

        data = cursor.fetchall()
        columns = ['id','date','time','block','iucr','location_desc', 
                   'arrest','district','year','latitude','longitude']
        
        df = pd.DataFrame(data, columns=columns)

        conx.commit()
        cursor.close()
        conx.close()

        # print(df.head())
        logging.info("Data fetched successfully")
        return df

    except Exception as err:
        logging.error(f"Error while getting data: {err}")
