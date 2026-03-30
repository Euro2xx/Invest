import yfinance as yf
from datetime import datetime, timedelta
import sqlite3
import json

# Kein API_KEY mehr nötig für yfinance


def get_stock_open_close(symbol: str, date: str):
    """Holt Open/Close Kurs für ein Symbol und ein Datum und füllt die Datenbank mit EPS- und Kursdaten.

    Args:
        symbol (str): Ticker-Symbol (z.B. TSLA, AAPL).
        date (str): Datum im Format YYYY-MM-DD.

    Returns:
        dict: {"symbol","date","open","close"} oder None bei Fehler. Füllt zudem die Datenbank.
    """
    symbol = symbol.strip().upper()
    print(f"Frage Tagesdaten für {symbol} am {date} ab...")

    try:
        # Datum parsen
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        
        # Ticker erstellen
        ticker = yf.Ticker(symbol)
        
        # Historische Daten für den Tag abrufen
        next_day = date_obj + timedelta(days=1)
        hist = ticker.history(start=date, end=next_day.strftime("%Y-%m-%d"))
        
        if hist.empty:
            print(f"Daten für das Datum {date} sind nicht verfügbar.")
            return None
        
        # Open und Close aus der ersten Zeile nehmen
        open_price = hist['Open'].iloc[0]
        close_price = hist['Close'].iloc[0]
        
        # EPS-Daten holen - verbesserte Extraktion mit mehr Quellen
        eps_estimate = None
        eps_actual = None
        
        try:
            print(f"Starte EPS-Extraktion für {symbol}...")
            
            # Methode 1: earnings_history (für Estimates)
            try:
                earnings = ticker.earnings_history
                print(f"earnings_history type: {type(earnings)}")
                if hasattr(earnings, 'empty') and not earnings.empty and len(earnings) > 0:
                    latest_earnings = earnings.iloc[-1]
                    
                    # Prüfe verschiedene mögliche Spaltennamen für EPS Estimate
                    eps_estimate_keys = ['EPS Estimate', 'epsEstimate', 'eps_estimate', 'Estimate', 'est_eps']
                    for key in eps_estimate_keys:
                        if key in latest_earnings and latest_earnings[key] is not None:
                            eps_estimate = latest_earnings[key]
                            print(f"EPS Estimate aus earnings_history ({key}): {eps_estimate}")
                            break
                    
                    # Prüfe verschiedene mögliche Spaltennamen für EPS Actual
                    eps_actual_keys = ['EPS Actual', 'epsActual', 'eps_actual', 'Actual', 'act_eps', 'EPS']
                    for key in eps_actual_keys:
                        if key in latest_earnings and latest_earnings[key] is not None:
                            eps_actual = latest_earnings[key]
                            print(f"EPS Actual aus earnings_history ({key}): {eps_actual}")
                            break
                    
                    print(f"Verfügbare Spalten in earnings_history: {latest_earnings.index.tolist()}")
            except Exception as e:
                print(f"earnings_history fehlgeschlagen: {e}")
            
            # Methode 2: Aus info-Dictionary für EPS Estimate
            if eps_estimate is None:
                try:
                    info = ticker.info
                    if info:
                        # Suche nach EPS-bezogenen Keys für Estimate
                        eps_estimate_keys = ['epsEstimate', 'forwardEps', 'epsTrailingTwelveMonths']
                        for key in eps_estimate_keys:
                            if key in info and info[key] is not None:
                                eps_estimate = info[key]
                                print(f"EPS Estimate aus info ({key}): {eps_estimate}")
                                break
                except Exception as e:
                    print(f"EPS Estimate aus info fehlgeschlagen: {e}")
            
            # Methode 3: Aus info-Dictionary (für Actual EPS)
            if eps_actual is None:
                try:
                    info = ticker.info
                    if info:
                        # Suche nach EPS-bezogenen Keys für Actual
                        eps_actual_keys = ['epsTrailingTwelveMonths', 'trailingEps', 'epsCurrentYear']
                        for key in eps_actual_keys:
                            if key in info and info[key] is not None:
                                eps_actual = info[key]
                                print(f"EPS Actual aus info ({key}): {eps_actual}")
                                break
                except Exception as e:
                    print(f"EPS aus info fehlgeschlagen: {e}")
            
            # Methode 4: Aus income_stmt (Net Income / Shares Outstanding)
            if eps_actual is None:
                try:
                    income_stmt = ticker.income_stmt
                    if income_stmt is not None and not income_stmt.empty:
                        # Net Income aus der letzten Periode
                        net_income = income_stmt.loc['Net Income'].iloc[0] if 'Net Income' in income_stmt.index else None
                        
                        # Shares Outstanding
                        shares_outstanding = None
                        try:
                            shares_outstanding = ticker.info.get('sharesOutstanding', None)
                        except:
                            pass
                        
                        if net_income is not None and shares_outstanding is not None:
                            eps_actual = net_income / shares_outstanding
                            print(f"EPS berechnet aus income_stmt: {eps_actual}")
                except Exception as e:
                    print(f"income_stmt Berechnung fehlgeschlagen: {e}")
            
            # Methode 4: quarterly_earnings als Fallback
            if eps_actual is None:
                try:
                    quarterly = ticker.quarterly_earnings
                    if quarterly is not None and not quarterly.empty and len(quarterly) > 0:
                        # EPS aus dem letzten Quartal
                        latest_quarter = quarterly.iloc[-1]
                        if hasattr(latest_quarter, 'get') and 'EPS' in latest_quarter:
                            eps_actual = latest_quarter['EPS']
                            print(f"EPS aus quarterly_earnings: {eps_actual}")
                        elif isinstance(latest_quarter, dict) and 'EPS' in latest_quarter:
                            eps_actual = latest_quarter['EPS']
                            print(f"EPS aus quarterly_earnings: {eps_actual}")
                except Exception as e:
                    print(f"quarterly_earnings fehlgeschlagen: {e}")
                    
        except Exception as e:
            print(f"Allgemeiner Fehler bei EPS-Extraktion: {e}")
        
        # Fallback-Werte setzen
        if eps_estimate is None:
            eps_estimate = "N/A"
        if eps_actual is None:
            eps_actual = "N/A"
            
        print(f"Final EPS: Estimate={eps_estimate}, Actual={eps_actual}")
        
        # Branche holen
        info = ticker.info
        branche = info.get('industry', 'Unbekannt')
        
        # Kurse für Zeitpunkte [-7 bis 7] holen
        zeitpunkte_preise = {}
        for offset in range(-7, 8):  # Alle Tage von -7 bis +7
            target_date = date_obj + timedelta(days=offset)
            target_next = target_date + timedelta(days=1)
            hist_target = ticker.history(start=target_date.strftime("%Y-%m-%d"), end=target_next.strftime("%Y-%m-%d"))
            if not hist_target.empty:
                zeitpunkte_preise[str(offset)] = round(hist_target['Close'].iloc[0], 2)
            else:
                zeitpunkte_preise[str(offset)] = None
        
        # JSON für Datenbank erstellen
        data_json = {
            "Unternehmen": symbol,
            "Branche": branche,
            "Datum": date,
            "Zeitpunkte": zeitpunkte_preise,  # Dict mit Tagen als Keys und Preisen als Values
            "EPS Estimate": eps_estimate,
            "EPS Actual": eps_actual
        }
        
        # In Datenbank speichern
        conn = sqlite3.connect('data/Datenbank_test.db')
        cursor = conn.cursor()
        cursor.execute("INSERT INTO json_data (data) VALUES (?)", (json.dumps(data_json),))
        conn.commit()
        conn.close()
        
        print(f"Daten für {symbol} wurden in der Datenbank gespeichert.")
        
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


