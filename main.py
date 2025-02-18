import requests
import datetime as dt
import pyodbc
import os
import json
# from dotenv import load_dotenv
import pandas as pd
# load_dotenv()

# Azure SQL Verbindungsdaten
SERVER = os.getenv("AZURE_SQL_SERVER")
DATABASE = os.getenv("AZURE_SQL_DATABASE")
USERNAME = os.getenv("AZURE_SQL_USER")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD")
DRIVER = "{ODBC Driver 17 for SQL Server}"

with open('config.json', 'r') as config_file:
    src_config = json.load(config_file)

#API_KEY = os.getenv("API_KEY")

API_KEY = "3f50f026fcd5a9997867c99b2c505d34"

def fetch_data():
    lat = src_config.get('lat')
    lon = src_config.get('lon')

    PARAMS = {
            "lat": lat,
            "lon": lon,
            "appid": API_KEY
    }

    api_url = src_config.get("URL")

    response = requests.get(api_url, params=PARAMS)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen der API: {response.status_code}")
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

def save_to_sql(dataframe):
    conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        cursor.execute(
                f"INSERT INTO weather_data (timestamp_unix, timestamp_dt, year, month, day, country, city, weather_main, weather_desc, temperature, humidity, cloudiness, longitude, latitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                dataframe['timestamp_unix'], 
                dataframe['timestamp_dt'],
                dataframe['year'], 
                dataframe['month'], 
                dataframe['day'], 
                dataframe['hour'],
                dataframe['country'], 
                dataframe['city'], 
                dataframe['weather_main'], 
                dataframe['weather_desc'], 
                dataframe['temperature'], 
                dataframe['humidity'], 
                dataframe['cloudiness'], 
                dataframe['longitude'], 
                dataframe['latitude']
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Daten erfolgreich gespeichert")
    except Exception as e:
        print(f"Datenbankfehler: {e}")



def main():
    data = fetch_data()
    df = build_df(data)
    df = parse_timestamps(df)
    save_to_sql(df)
    print(df)


if __name__ == "__main__":
    main()