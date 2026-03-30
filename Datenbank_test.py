import sqlite3
import json
from businesslogik import DatabaseManager



# Verbindung zur SQLite-Datenbank herstellen (erstellt 'Datenbank_test.db', wenn sie nicht existiert)
conn = sqlite3.connect('data/Datenbank_test.db')
cursor = conn.cursor()

# Tabelle für JSON-Daten erstellen (falls sie nicht existiert)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS json_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT  -- JSON-Daten als Text speichern
    )
''')

# Beispiel: JSON-Daten einfügen
sample_json = json.dumps({
    "Unternehmen": "Beispiel-Unternehmen",
    "Branche": "Technologie",
    "Datum": "2026-03-30",
    "Zeitpunkte": [-7-+7],
    "EPS Estimate": 123.45,
    "EPS Actual": 120.0
})
cursor.execute("INSERT INTO json_data (data) VALUES (?)", (sample_json,))

# Änderungen speichern und Verbindung schließen
conn.commit()
conn.close()

print("SQLite-Datenbank 'Datenbank_test.db' wurde erstellt und Beispiel-Daten wurden eingefügt.")
