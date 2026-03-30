import sqlite3
import json


class DatabaseManager:
    """Hilfsklasse zum Verwalten und Auslesen der SQLite-Datenbank"""
    
    def __init__(self, db_path='data/Datenbank_test.db'):
        self.db_path = db_path
    
    def get_all_data(self):
        """Gibt alle Datensätze zurück
        
        Returns:
            list: Liste von Dictionaries mit allen Daten aus der Datenbank
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, data FROM json_data")
        rows = cursor.fetchall()
        conn.close()
        return [{"id": row[0], **json.loads(row[1])} for row in rows]
    
    def get_by_symbol(self, symbol):
        """Filtert Daten nach Symbol (Unternehmen)
        
        Args:
            symbol (str): Ticker-Symbol (z.B. TSLA, AAPL)
        
        Returns:
            list: Liste von Dictionaries für das Symbol
        """
        all_data = self.get_all_data()
        return [item for item in all_data if item['Unternehmen'] == symbol.upper()]
    
    def get_by_date(self, date):
        """Filtert Daten nach Datum
        
        Args:
            date (str): Datum im Format YYYY-MM-DD
        
        Returns:
            list: Liste von Dictionaries für das Datum
        """
        all_data = self.get_all_data()
        return [item for item in all_data if item['Datum'] == date]
    
    def get_by_symbol_and_date(self, symbol, date):
        """Filtert Daten nach Symbol und Datum
        
        Args:
            symbol (str): Ticker-Symbol
            date (str): Datum im Format YYYY-MM-DD
        
        Returns:
            dict: Dictionary mit Daten oder None wenn nicht gefunden
        """
        all_data = self.get_all_data()
        result = [item for item in all_data 
                 if item['Unternehmen'] == symbol.upper() and item['Datum'] == date]
        return result[0] if result else None
    
    def get_count(self):
        """Gibt die Anzahl der Datensätze in der Datenbank zurück
        
        Returns:
            int: Anzahl der Datensätze
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM json_data")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def delete_by_symbol_and_date(self, symbol, date):
        """Löscht einen Datensatz nach Symbol und Datum
        
        Args:
            symbol (str): Ticker-Symbol
            date (str): Datum im Format YYYY-MM-DD
        
        Returns:
            bool: True wenn gelöscht, False wenn nicht gefunden
        """
        try:
            # Erst alle Daten laden
            all_data = self.get_all_data()
            
            # Den zu löschenden Eintrag finden
            to_delete = None
            for item in all_data:
                if item['Unternehmen'] == symbol.upper() and item['Datum'] == date:
                    to_delete = item['id']
                    break
            
            if to_delete is None:
                return False
            
            # Datensatz löschen
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM json_data WHERE id = ?", (to_delete,))
            conn.commit()
            conn.close()
            
            print(f"✓ Datensatz für {symbol} vom {date} gelöscht")
            return True
            
        except Exception as e:
            print(f"✗ Fehler beim Löschen: {e}")
            return False
    
    def delete_by_symbol(self, symbol):
        """Löscht alle Datensätze für ein Symbol
        
        Args:
            symbol (str): Ticker-Symbol
        
        Returns:
            int: Anzahl der gelöschten Datensätze
        """
        try:
            all_data = self.get_all_data()
            deleted_ids = []
            
            for item in all_data:
                if item['Unternehmen'] == symbol.upper():
                    deleted_ids.append(item['id'])
            
            if not deleted_ids:
                return 0
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            for id_val in deleted_ids:
                cursor.execute("DELETE FROM json_data WHERE id = ?", (id_val,))
            conn.commit()
            conn.close()
            
            print(f"✓ {len(deleted_ids)} Datensätze für {symbol} gelöscht")
            return len(deleted_ids)
            
        except Exception as e:
            print(f"✗ Fehler beim Löschen: {e}")
            return 0
    
    def delete_all(self):
        """Löscht alle Datensätze aus der Datenbank
        
        Returns:
            int: Anzahl der gelöschten Datensätze
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM json_data")
            count = cursor.fetchone()[0]
            
            cursor.execute("DELETE FROM json_data")
            conn.commit()
            conn.close()
            
            print(f"✓ Alle {count} Datensätze wurden gelöscht")
            return count
            
        except Exception as e:
            print(f"✗ Fehler beim Löschen aller Datensätze: {e}")
            return 0


