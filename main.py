import requests
#import pyodbc
import os
#import time
import json
from dotenv import load_dotenv

# API URL & Header
load_dotenv()

# # Azure SQL Verbindungsdaten
# SERVER = os.getenv("AZURE_SQL_SERVER")
# DATABASE = os.getenv("AZURE_SQL_DATABASE")
# USERNAME = os.getenv("AZURE_SQL_USER")
# PASSWORD = os.getenv("AZURE_SQL_PASSWORD")
# DRIVER = "{ODBC Driver 17 for SQL Server}"

with open('config.json', 'r') as config_file:
    src_config = json.load(config_file)

API_KEY = os.getenv("API_KEY")

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

def main():
    data = fetch_data()
    print(data)


if __name__ == "__main__":
    main()


# def save_to_sql(data):
#     """Speichert Daten in Azure SQL"""
#     conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
#     try:
#         conn = pyodbc.connect(conn_str)
#         cursor = conn.cursor()
        
#         for item in data:
#             cursor.execute(
#                 "INSERT INTO my_table (id, name, value) VALUES (?, ?, ?)",
#                 item["id"], item["name"], item["value"]
#             )

#         conn.commit()
#         cursor.close()
#         conn.close()
#         print("Daten erfolgreich gespeichert")
#     except Exception as e:
#         print(f"Datenbankfehler: {e}")

# if __name__ == "__main__":
#     data = fetch_data()
#     if data:
#         save_to_sql(data)
