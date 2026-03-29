import yfinance as yf
from datetime import datetime, timedelta

# Kein API_KEY mehr nötig für yfinance


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

    try:
        # Datum parsen
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        next_day = date_obj + timedelta(days=1)
        
        # Ticker erstellen
        ticker = yf.Ticker(symbol)
        
        # Historische Daten für den Tag abrufen
        hist = ticker.history(start=date, end=next_day.strftime("%Y-%m-%d"))
        
        if hist.empty:
            print(f"Daten für das Datum {date} sind nicht verfügbar.")
            return None
        
        # Open und Close aus der ersten Zeile nehmen
        open_price = hist['Open'].iloc[0]
        close_price = hist['Close'].iloc[0]
        
        return {
            "symbol": symbol,
            "date": date,
            "open": round(open_price, 2),
            "close": round(close_price, 2),
        }
    
    except Exception as e:
        print(f"Fehler beim Abrufen der Daten: {e}")
        return None


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


