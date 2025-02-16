import requests
import pandas as pd

def get_msci_world_data():
    ticker = "XWD.TO" 
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        'range': "1mo",
        'interval': "1h"
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    
    result = data["chart"]["result"][0]
    timestamps = result["timestamp"]
    quotes = result["indicators"]["quote"][0]  # Quote enthält Preis- und Volumendaten

    # Erstelle den DataFrame
    df = pd.DataFrame({
        "timestamp": timestamps,
        "open": quotes["open"],
        "high": quotes["high"],
        "low": quotes["low"],
        "close": quotes["close"],
        "volume": quotes["volume"]
    })

    # Optional: Konvertiere den Timestamp in ein Datetime-Format
    df["date"] = pd.to_datetime(df["timestamp"], unit="s")

    # Spaltenreihenfolge anpassen
    df = df[["date", "open", "high", "low", "close", "volume"]]

    # Ausgabe des DataFrame
    print(df)

if __name__ == "__main__":
    get_msci_world_data()
