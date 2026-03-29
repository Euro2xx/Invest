import requests
import json

API_KEY = "UTWEJGJXW2VDETQ8"
BASE_URL = "https://www.alphavantage.co/query"


def get_stock_open_close(symbol: str, date: str):
    """Holt Open/Close Kurs für ein Symbol und ein Datum.

    Args:
        symbol (str): Ticker-Symbol (z.B. TSLA, AAPL).
        date (str): Datum im Format YYYY-MM-DD.

    Returns:
        dict: {"symbol","date","open","close"} oder None bei Fehler.
    """
    symbol = symbol.strip().upper()
    print(f"Frage Tagesdaten für {symbol} am {date} ab...")

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": "full",
        "apikey": API_KEY,
    }

    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if "Error Message" in data:
        print("Fehler: Ungültiges Symbol oder Anfragefehler.", data.get("Error Message"))
        return None

    if "Time Series (Daily)" not in data:
        print("Fehler: Zeitreihen-Daten nicht gefunden.", data)
        return None

    time_series = data["Time Series (Daily)"]

    if date not in time_series:
        print(f"Daten für das Datum {date} sind nicht verfügbar. Verfügbare Daten begrenzt.")
        return None

    day_data = time_series[date]
    open_price = day_data.get("1. open", "N/A")
    close_price = day_data.get("4. close", "N/A")

    return {
        "symbol": symbol,
        "date": date,
        "open": open_price,
        "close": close_price,
    }


if __name__ == "__main__":
    print("=" * 50)
    print("Aktienkurs am Tagesanfang und Tagesende")
    print("=" * 50)

    symbol = input("Gib das Tickersymbol ein (z.B. TSLA): ").strip()
    date = input("Gib das Datum ein (YYYY-MM-DD): ").strip()

    quote = get_stock_open_close(symbol, date)
    if quote:
        print("\nErgebnisse:")
        print(f"Symbol: {quote['symbol']}")
        print(f"Datum: {quote['date']}")
        print(f"Open: {quote['open']}")
        print(f"Close: {quote['close']}")
        print("=" * 50)
    else:
        print("Keine Daten verfügbar.")


