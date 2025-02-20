import requests
import datetime as dt
import pyodbc
import os
import json
from dotenv import load_dotenv
import pandas as pd
import logging

load_dotenv()

# Azure SQL Verbindungsdaten
SERVER = os.getenv("AZURE_SQL_SERVER")
DATABASE = os.getenv("AZURE_SQL_DATABASE")
USERNAME = os.getenv("AZURE_SQL_USER")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD")
DRIVER = "{ODBC Driver 17 for SQL Server}"

with open('/app/config.json', 'r') as config_file:
    src_config = json.load(config_file)

API_KEY = os.getenv("API_KEY")

logging.basicConfig(filename='/var/log/main_py.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        response.raise_for_status()  # Will trigger an exception for 4xx/5xx responses
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

def save_to_sql(dataframe):
    conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO weather_data 
            (timestamp_unix, timestamp_dt, year, month, day, hour, country, city, weather_main, weather_desc, temperature, humidity, cloudiness, longitude, latitude) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            int(dataframe['timestamp_unix'].iloc[0]),
            dataframe['timestamp_dt'].iloc[0],
            int(dataframe['year'].iloc[0]),
            int(dataframe['month'].iloc[0]),
            int(dataframe['day'].iloc[0]),
            int(dataframe['hour'].iloc[0]),
            dataframe['country'].iloc[0],
            dataframe['city'].iloc[0],
            dataframe['weather_main'].iloc[0],
            dataframe['weather_desc'].iloc[0],
            float(dataframe['temperature'].iloc[0]),
            int(dataframe['humidity'].iloc[0]),
            int(dataframe['cloudiness'].iloc[0]),
            float(dataframe['longitude'].iloc[0]),
            float(dataframe['latitude'].iloc[0])
        )

        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Daten erfolgreich gespeichert")
    except Exception as e:
        logging.error(f"Verbindungsfehler zu SQL: {e}")



def main():
    data = fetch_data()
    df = build_df(data)
    df = parse_timestamps(df)
    df = parse_temperature(df)
    save_to_sql(df)
    return df


if __name__ == "__main__":
    main()