import json
from typing import List, Dict, Any
from api import get_stock_open_close
from datetime import datetime

def process_batch_api(batch_input: str) -> Dict[str, Any]:
    """
    Verarbeitet eine Batch-API-Anfrage mit JSON-Input.
    
    Args:
        batch_input (str): JSON-String im Format:
            '[{"symbol": "JPM", "date": "2026-01-15"}, {"symbol": "BAC", "date": "2026-01-15"}]'
            oder Alternative Formate: '[[JPM, 2026-01-15], [BAC, 2026-01-15]]'
    
    Returns:
        Dict: Enthält:
            - "success": Anzahl erfolgreich verarbeiteter Anfragen
            - "failed": Anzahl fehlgeschlagener Anfragen
            - "results": Liste der Ergebnisse
            - "errors": Liste der Fehler
    """
    results = []
    errors = []
    success_count = 0
    failed_count = 0
    
    try:
        # Parse JSON Input
        data = json.loads(batch_input)
        
        # Validiere und konvertiere Input-Format
        if not isinstance(data, list):
            raise ValueError("Input muss eine Liste sein")
        
        print(f"\n{'='*60}")
        print(f"Starte Batch-Verarbeitung mit {len(data)} Einträgen")
        print(f"{'='*60}\n")
        
        # Verarbeite jeden Eintrag
        for index, item in enumerate(data, 1):
            try:
                # Extrahiere Symbol und Datum je nach Input-Format
                if isinstance(item, dict):
                    symbol = item.get('symbol') or item.get('Symbol')
                    date = item.get('date') or item.get('Date')
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    symbol = item[0]
                    date = item[1]
                else:
                    raise ValueError(f"Ungültiges Format für Eintrag {index}")
                
                if not symbol or not date:
                    raise ValueError(f"Symbol oder Datum fehlt bei Eintrag {index}")
                
                print(f"[{index}/{len(data)}] Verarbeite {symbol} für {date}...")
                
                # Rufe API auf
                result = get_stock_open_close(str(symbol), str(date))
                
                if result:
                    results.append(result)
                    success_count += 1
                    print(f"✓ Erfolgreich: {symbol} - Open: {result['open']}, Close: {result['close']}\n")
                else:
                    failed_count += 1
                    error_msg = f"Keine Daten für {symbol} am {date}"
                    errors.append({"symbol": symbol, "date": date, "error": error_msg})
                    print(f"✗ Fehler: {error_msg}\n")
                    
            except Exception as e:
                failed_count += 1
                error_msg = f"Exception für {item}: {str(e)}"
                errors.append({"item": str(item), "error": error_msg})
                print(f"✗ Fehler: {error_msg}\n")
        
        # Zusammenfassung
        print(f"\n{'='*60}")
        print("BATCH-VERARBEITUNG ABGESCHLOSSEN")
        print(f"{'='*60}")
        print(f"Erfolgreich: {success_count}/{len(data)}")
        print(f"Fehler: {failed_count}/{len(data)}")
        print(f"{'='*60}\n")
        
        return {
            "success": success_count,
            "failed": failed_count,
            "total": len(data),
            "results": results,
            "errors": errors,
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError as e:
        print(f"JSON-Parse-Fehler: {e}")
        return {
            "success": 0,
            "failed": 0,
            "results": [],
            "errors": [{"error": f"JSON-Parse-Fehler: {str(e)}"}],
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Allgemeiner Fehler: {e}")
        return {
            "success": 0,
            "failed": 0,
            "results": [],
            "errors": [{"error": str(e)}],
            "timestamp": datetime.now().isoformat()
        }


def process_batch_from_file(filepath: str) -> Dict[str, Any]:
    """
    Verarbeitet Batch-API-Anfragen aus einer JSON-Datei.
    
    Args:
        filepath (str): Pfad zur JSON-Datei
    
    Returns:
        Dict: Ergebnis der Batch-Verarbeitung
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            batch_input = f.read()
        return process_batch_api(batch_input)
    except FileNotFoundError:
        return {
            "success": 0,
            "failed": 0,
            "results": [],
            "errors": [{"error": f"Datei nicht gefunden: {filepath}"}],
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "success": 0,
            "failed": 0,
            "results": [],
            "errors": [{"error": str(e)}],
            "timestamp": datetime.now().isoformat()
        }


if __name__ == "__main__":
    # Beispiel 1: Batch mit dict-Format (Echte Earnings-Termine Januar 2026)
    batch_json_dict = '''[
        {"symbol": "JPM", "date": "2026-01-16"},
        {"symbol": "BAC", "date": "2026-01-14"},
        {"symbol": "C", "date": "2026-01-16"},
        {"symbol": "WFC", "date": "2026-01-16"},
        {"symbol": "GS", "date": "2026-01-21"},
        {"symbol": "MS", "date": "2026-01-21"},
        {"symbol": "PNC", "date": "2026-01-16"},
        {"symbol": "USB", "date": "2026-01-21"},
        {"symbol": "CFG", "date": "2026-01-21"},
        {"symbol": "SCHW", "date": "2026-01-21"}
    ]'''
    
    # Beispiel 2: Batch mit array-Format
    batch_json_list = '''[
        ["JPM", "2026-01-16"],
        ["BAC", "2026-01-14"],
        ["C", "2026-01-16"]
    ]'''
    
    print("Starte Batch-Verarbeitung mit Beispieldaten...\n")
    result = process_batch_api(batch_json_dict)
    
    print("\nRESULTAT:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
