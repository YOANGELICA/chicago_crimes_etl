import psycopg2 as psy
import json


def create_db_connection():
    with open('config_db.json') as config_json:
        config = json.load(config_json)
    conx = psy.connect(**config) 
    return conx

#### CRIMES
def create_table_crimes():
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
        updated_on TIMESTAMP,
        location POINT
    );
""")

    conx.commit()
    mycursor.close()
    conx.close()
    return "ok"

def describe_crimes():
    conx = create_db_connection()
    mycursor = conx.cursor()
    mycursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'crimes';")

    description_crimes = mycursor.fetchall()

    conx.commit()
    mycursor.close()
    conx.close()

    return description_crimes

def insert_info_crimes(df):
    conx = create_db_connection()
    mycursor = conx.cursor()
    
    for _, i in df.iterrows():
        insert = """INSERT INTO crimes 
        (id,date, time, block, iucr, location_desc, arrest, 
        district, year, updated_on, location) 
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s))"""

        datos = (
            i['id'],
            i['date'],
            i['time'],
            i['block'],
            i['iucr'],
            i['location_desc'],
            i['arrest'],
            i['district'],
            i['year'],
            i['updated_on'],
            i['location']
        )

        mycursor.execute(insert, datos)
    conx.commit()
    mycursor.close()
    conx.close()
    return "ok"

#### IUCR
def create_table_iucr():
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

def describe_iucr():
    conx = create_db_connection()
    mycursor = conx.cursor()

    mycursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'codes';")


    description_iucr = mycursor.fetchall()

    mycursor.close()
    return description_iucr

def insert_info_iucr(df):
    conx = create_db_connection()
    mycursor = conx.cursor()
    
    for _, i in df.iterrows():
        insert = """INSERT INTO codes 
        (iucr, primary_description, 
        secondary_description, index_code, active) 
        VALUES (%s, %s, %s, %s, %s)"""

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

#### DATES

def create_table_dates():
    conx = create_db_connection()
    mycursor = conx.cursor()

    mycursor.execute("""CREATE TABLE IF NOT EXISTS dates (
                        date TIMESTAMP,
                        date_id VARCHAR(10) PRIMARY KEY,
                        year INT,
                        month VARCHAR(10),
                        day_week VARCHAR(10)
                        );""")

    conx.commit()
    mycursor.close()
    conx.close()
    return "ok"

def describe_dates():
    conx = create_db_connection()
    mycursor = conx.cursor()

    mycursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dates';")


    description_iucr = mycursor.fetchall()

    mycursor.close()
    return description_iucr

def insert_info_dates(df):
    conx = create_db_connection()
    mycursor = conx.cursor()
    
    for _, i in df.iterrows():
        insert = """INSERT INTO dates 
        (date, date_id, year, month, day_week) 
        VALUES (%s, %s, %s, %s, %s)"""

        datos = (
            i['date'],
            i['date_id'], 
            i['year'], 
            i['month'], 
            i['day_week']
        )

        mycursor.execute(insert, datos)
    conx.commit()
    mycursor.close()
    conx.close()
    return "ok"
