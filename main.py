import requests
import datetime as dt
import pyodbc
import os
import json
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import logging

#logging.basicConfig(filename='/var/log/main_py_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

SERVER = os.getenv("AZURE_SQL_SERVER")
DATABASE = os.getenv("AZURE_SQL_DATABASE")
USERNAME = os.getenv("AZURE_SQL_USER")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD")
DRIVER = "{ODBC Driver 17 for SQL Server}"
API_KEY = os.getenv("API_KEY")

with open('config.json', 'r') as config_file:
    src_config = json.load(config_file)

def fetch_data():
    lat = src_config.get('lat')
    lon = src_config.get('lon')

    PARAMS = {
            "lat": lat,
            "lon": lon,
            "appid": API_KEY
    }

    api_url = src_config.get("URL")

    try:
        response = requests.get(api_url, params=PARAMS)
        response.raise_for_status() 
        logging.info(f"API-Aufruf erfolgreich: {api_url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Fehler beim Abrufen der API: {e}")
        return None

def build_df(data_input):
    df = pd.DataFrame([{
        'timestamp_unix' : data_input['dt'],
        'country' : data_input['sys']['country'],
        'city' : data_input['name'],
        'weather_main' : data_input['weather'][0]['main'],
        'weather_desc' : data_input['weather'][0]['description'],
        'temperature' : data_input['main']['temp'],
        'humidity' : data_input['main']['humidity'],
        'cloudiness': data_input['clouds']['all'],
        'longitude' : data_input['coord']['lon'],
        'latitude' : data_input['coord']['lat']
    }])
    return df

def parse_timestamps(dataframe):
    timestamp = dt.datetime.fromtimestamp(dataframe['timestamp_unix'].iloc[0])
    dataframe['timestamp_dt'] = timestamp
    dataframe['year'] = timestamp.year
    dataframe['month'] = timestamp.month
    dataframe['day'] = timestamp.day
    dataframe['hour'] = timestamp.hour
    return dataframe

def parse_temperature(dataframe):
    dataframe['temperature'] = dataframe['temperature'] - 273.15
    return dataframe

def create_sql_statement(dataframe):
    table_name = 'weather_data'
    columns = list(dataframe.columns.values)
    placeholders = ",".join(["?" for _ in columns])

    sql = f'INSERT INTO {table_name} ({",".join(columns)}) VALUES ({placeholders})'
    
    return sql

def save_to_sql(dataframe, sql_statement):
    conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        values = [val.item() if isinstance(val, (np.int64, np.float64)) else val for val in list(dataframe.iloc[0])]
        print(values)
        cursor.execute(sql_statement, values) 

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data saved sucessfully")
    except Exception as e:
        logging.error(f"Connection_error: {e}")

def main():
    data = fetch_data()
    df = build_df(data)
    df = parse_timestamps(df)
    df = parse_temperature(df)
    sql_insert = create_sql_statement(df)
    save_to_sql(df, sql_insert)
    print(f"Data has been saved with the following SQL Statement: {sql_insert}")

if __name__ == "__main__":
    main()