import pandas as pd


def drop_rows_out_of_chicago(df):
    df= df[~((df['Latitude'] == 36.619446395) & (df['Longitude'] == -91.686565684))]
    return df

def split_datetime(df):
    # turning date column into datetime to extract the time and create the time column
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y %I:%M:%S %p')
    df['time'] = df['date'].apply(lambda x: x.time())
    df=df.drop('Date', axis=1) #Como cambiamos Date a date, creó una nueva columna, así que tocaba eliminarla para que no quedara extra
    return df

def move_time(df):
    move_time = df.pop('time')
    df.insert(2,'time', move_time)
    return df

def move_date(df):
    move_date = df.pop('date')
    df.insert(2,'date', move_date)
    return df

# def drop_unnamed0(df):
#     df.drop('Unnamed: 0', inplace=True)
#     return df

def change_updated_on_format(df):
    # changing updated_on format
    df['Updated On'] = pd.to_datetime(df['Updated On'], format='%m/%d/%Y %I:%M:%S %p')
    return df

def convert_dtype(df):
    # taking just the date part, converting into dtype object
    df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y %I:%M:%S %p').dt.date
    return df

def replace_nulls(df):
    # replacing nulls
    df = df.fillna({'Case Number': 'NULL', 'Location Description': 'NULL'})
    df = df.fillna({'District': 0, 'Ward': 0, 'Community Area': 0}) 
    return df

def change_dtype_columns(df):
    cols = ['District', 'Ward', 'Community Area', 'Arrest', 'Domestic']
    df[cols] = df[cols].astype(int)
    return df

def change_columns_names(df):
    df.columns = ["id","case_number","date", "time", "block","iucr","primary_type","description","location_desc","arrest","domestic","beat","district","ward","community_area","fbi_code", "x_coord","y_coord","year","updated_on","latitude","longitude", "location"]
    return df

def create_point(df):
    not_null_mask = ~df['latitude'].isnull() & ~df['longitude'].isnull()
    # Apply the transformation only for non-null values
    df.loc[not_null_mask, 'location'] = df[not_null_mask].apply(lambda row: f"POINT({row['latitude']} {row['longitude']})", axis=1)
    return df

def drop_na_location(df):
    df= df.dropna(subset=['location'])
    return df

def drop_columns(df):
    columns_to_drop = ['case_number', 'domestic', 'beat', 'ward', 'community_area', 'fbi_code', 'x_coord', 'y_coord', 'primary_type', 'description']
    df.drop(columns=columns_to_drop, inplace=True)
    return df

def drop_columns_newdata(df):
    df = df.iloc[:, :-10]
    return df

def split_datetime_newdata(df):
    df['time'] = df['date'].str.split('T').str[1]
    df['date'] = df['date'].str.split('T').str[0]
    return df

def replace_nulls_newdata(df):
    # replacing nulls
    df = df.fillna({'location_description': 'null', 'ward': 0, 'x_coordinate': 0,'y_coordinate': 0})
    return df

def change_columns_dtype_newdata(df):
    # changing columns dtype
    cols = ['district', 'arrest', 'domestic', 'ward', 'community_area', 'x_coordinate', 'y_coordinate']
    df[cols] = df[cols].astype(int)
    return df

