import streamlit as st
from api import get_stock_open_close
from data_auto import process_batch_api
from visualisation import DataVisualization
from datetime import datetime
from businesslogik import DatabaseManager
import json
import pandas as pd


def format_data_for_display(data_list):
    """Konvertiert alle nested Datentypen zu JSON-Strings für die Dataframe-Darstellung"""
    import pandas as pd
    
    formatted = []
    for item in data_list:
        formatted_item = item.copy()
        
        for key, value in formatted_item.items():
            # Alle nested Strukturen (dict, list) zu JSON-String konvertieren
            if isinstance(value, (dict, list)):
                formatted_item[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                formatted_item[key] = "N/A"
        
        formatted.append(formatted_item)
    
    # In DataFrame konvertieren und alle Spalten zu String machen
    df = pd.DataFrame(formatted)
    return df.astype(str)




st.title("Aktienkurs Abfrage")

st.write("Gib das Tickersymbol und das Datum ein, um Kurse zu erhalten.")

symbol = st.text_input("Tickersymbol (z.B. TSLA)", value="TSLA")

date_input = st.date_input("Datum", value=datetime.today())

if st.button("Daten abrufen"):
    # Datum formatieren
    date_str = date_input.strftime("%Y-%m-%d")
    
    # Funktion aufrufen
    quote = get_stock_open_close(symbol, date_str)
    
    if quote:
        st.success("Daten erfolgreich abgerufen!")
        st.write(f"**Symbol:** {quote['symbol']}")
        st.write(f"**Datum:** {quote['date']}")
        st.write(f"**Open:** {quote['open']}")
        st.write(f"**Close:** {quote['close']}")
    else:
        st.error("Keine Daten verfügbar für das angegebene Symbol und Datum.")


st.divider()
st.subheader("Batch-Verarbeitung")

batch_input_method = st.radio("Eingabemethode:", ["JSON eingeben", "JSON-Beispiel verwenden"])

if batch_input_method == "JSON-Beispiel verwenden":
    sample_json = '''[
    {"symbol": "JPM", "date": "2026-01-16"},
    {"symbol": "BAC", "date": "2026-01-14"},
    {"symbol": "C", "date": "2026-01-16"},
    {"symbol": "WFC", "date": "2026-01-16"},
    {"symbol": "GS", "date": "2026-01-21"},
    {"symbol": "MS", "date": "2026-01-21"},
    {"symbol": "PNC", "date": "2026-01-16"},
    {"symbol": "USB", "date": "2026-01-21"},
    {"symbol": "CFG", "date": "2026-01-21"},
    {"symbol": "MTB", "date": "2026-01-21"},
    {"symbol": "EFSC", "date": "2026-01-26"},
    {"symbol": "NBTB", "date": "2026-01-26"},
    {"symbol": "SCHW", "date": "2026-01-21"},
    {"symbol": "IBKR", "date": "2026-01-20"},
    {"symbol": "SOFI", "date": "2026-01-26"},
    {"symbol": "BLK", "date": "2026-01-16"},
    {"symbol": "TRV", "date": "2026-01-21"},
    {"symbol": "AXP", "date": "2026-01-23"},
    {"symbol": "AXS", "date": "2026-01-28"}
]'''
    batch_input = st.text_area("JSON-Batch-Input:", value=sample_json, height=300)
else:
    batch_input = st.text_area("JSON-Batch-Input:", height=150, 
                             placeholder='[{"symbol": "JPM", "date": "2026-01-15"}, {"symbol": "BAC", "date": "2026-01-15"}]')

col1, col2 = st.columns(2)

with col1:
    if st.button("Batch verarbeiten"):
        if batch_input.strip():
            with st.spinner("Verarbeite Batch..."):
                result = process_batch_api(batch_input)
                
                # Erfolgs-/Fehlerquote anzeigen
                col_success, col_failed, col_total = st.columns(3)
                with col_success:
                    st.metric("✓ Erfolgreich", result['success'])
                with col_failed:
                    st.metric("✗ Fehler", result['failed'])
                with col_total:
                    st.metric("Total", result['total'])
                
                # Detaillierte Ergebnisse
                if result['results']:
                    st.subheader("Erfolgreiche Abfragen")
                    results_df = pd.DataFrame(result['results'])
                    st.dataframe(results_df)
                
                if result['errors']:
                    st.subheader("Fehler")
                    for error in result['errors']:
                        st.error(f"{error}")
                
                # JSON-Download anbieten
                st.subheader("Vollständiges Ergebnis")
                st.json(result)
        else:
            st.error("Bitte geben Sie ein gültiges JSON ein.")

with col2:
    if st.button("Format-Hilfe anzeigen"):
        st.info("""
        **Unterstützte JSON-Formate:**
        
        **Format 1 (Dict):**
        ```json
        [
            {"symbol": "JPM", "date": "2026-01-15"},
            {"symbol": "BAC", "date": "2026-01-15"}
        ]
        ```
        
        **Format 2 (Array):**
        ```json
        [
            ["JPM", "2026-01-15"],
            ["BAC", "2026-01-15"]
        ]
        ```
        """)

st.divider()
st.subheader("Datenbankabfrage")

# Filteroptionen
filter_option = st.radio("Filtern nach:", ["Alle Daten", "Nach Symbol", "Nach Datum", "Nach Symbol & Datum"])

db = DatabaseManager()

if filter_option == "Alle Daten":
    data = db.get_all_data()
    st.write("**Alle gespeicherten Daten:**")
    if data:
        st.dataframe(format_data_for_display(data), use_container_width=True)
    else:
        st.info("Keine Daten in der Datenbank vorhanden.")

elif filter_option == "Nach Symbol":
    filter_symbol = st.text_input("Symbol eingeben:", value=symbol)
    if filter_symbol:
        data = db.get_by_symbol(filter_symbol)
        st.write(f"**Daten für {filter_symbol.upper()}:**")
        if data:
            st.dataframe(format_data_for_display(data), use_container_width=True)
        else:
            st.info(f"Keine Daten für {filter_symbol.upper()} gefunden.")

elif filter_option == "Nach Datum":
    filter_date = st.date_input("Datum wählen:", value=date_input)
    filter_date_str = filter_date.strftime("%Y-%m-%d")
    data = db.get_by_date(filter_date_str)
    st.write(f"**Daten für {filter_date_str}:**")
    if data:
        st.dataframe(format_data_for_display(data), use_container_width=True)
    else:
        st.info(f"Keine Daten für {filter_date_str} gefunden.")

elif filter_option == "Nach Symbol & Datum":
    filter_symbol = st.text_input("Symbol eingeben:", value=symbol)
    filter_date = st.date_input("Datum wählen:", value=date_input)
    filter_date_str = filter_date.strftime("%Y-%m-%d")
    if filter_symbol and filter_date_str:
        data = db.get_by_symbol_and_date(filter_symbol, filter_date_str)
        st.write(f"**Daten für {filter_symbol.upper()} am {filter_date_str}:**")
        if data:
            st.json(data)
        else:
            st.info(f"Keine Daten für {filter_symbol.upper()} am {filter_date_str} gefunden.")

st.divider()
st.subheader("�️ Datenverwaltung & Löschen")

delete_option = st.radio("Löschen:", ["Einzelnen Datensatz löschen", "Alle Daten eines Symbols löschen", "Gesamte Datenbank löschen"])

if delete_option == "Einzelnen Datensatz löschen":
    st.write("**Löscht einen spezifischen Datensatz (Symbol + Datum)**")
    col1, col2 = st.columns(2)
    
    with col1:
        delete_symbol = st.text_input("Symbol:", key="delete_symbol")
    with col2:
        delete_date = st.date_input("Datum:", key="delete_date")
    
    if st.button("Datensatz löschen 🗑️"):
        if delete_symbol:
            delete_date_str = delete_date.strftime("%Y-%m-%d")
            success = db.delete_by_symbol_and_date(delete_symbol, delete_date_str)
            if success:
                st.success(f"✓ Datensatz für {delete_symbol} vom {delete_date_str} wurde gelöscht!")
                st.rerun()
            else:
                st.error(f"✗ Kein Datensatz für {delete_symbol} vom {delete_date_str} gefunden.")

elif delete_option == "Alle Daten eines Symbols löschen":
    st.write("**Löscht alle Datensätze für ein bestimmtes Symbol**")
    delete_symbol = st.text_input("Symbol eingeben:", key="delete_all_symbol")
    
    if st.button("Alle Daten dieses Symbols löschen 🗑️"):
        if delete_symbol:
            count = db.delete_by_symbol(delete_symbol)
            if count > 0:
                st.success(f"✓ {count} Datensätze für {delete_symbol} wurden gelöscht!")
                st.rerun()
            else:
                st.warning(f"⚠️ Keine Daten für {delete_symbol} gefunden.")

elif delete_option == "Gesamte Datenbank löschen":
    st.warning("⚠️ **Warnung:** Dies löscht ALLE Datensätze und kann nicht rückgängig gemacht werden!")
    
    col1, col2 = st.columns(2)
    with col1:
        confirm_checkbox = st.checkbox("Ich bin mir sicher und möchte alle Daten löschen")
    
    with col2:
        if confirm_checkbox:
            if st.button("✓ Gesamte Datenbank LÖSCHEN", type="primary"):
                count = db.delete_all()
                st.success(f"✓ Alle {count} Datensätze wurden gelöscht! Die Datenbank ist leer.")
                st.rerun()
    
    if not confirm_checkbox:
        st.info("Markiere die Checkbox oben, um diese Aktion zu bestätigen.")

st.divider()
st.subheader("📊 Datenvisualisierung")

# Datenbankinfo - zeige aktuelle Anzahl
info_col1, info_col2 = st.columns([3, 1])
with info_col2:
    st.metric("Datensätze", db.get_count())

# Laden der Visualisierungsklasse
viz = DataVisualization()

# Visualisierungsoptionen
viz_option = st.radio("Visualisierungstyp:", [
    "Überlagerte Aktienkurse",
    "Einzelne Unternehmen",
    "Vergleich am Ereignis-Tag",
    "Zusammenfassungsstatistiken",
    "Prozentuale Änderung & EPS",
    "Earnings Metriken"
])

if viz_option == "Überlagerte Aktienkurse":
    st.write("Alle Aktienkurse überlagert (normalisiert auf Index = 100), um Trends leicht zu vergleichen:")
    fig = viz.plot_overlayed_prices_plotly()
    if fig:
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Keine Daten zur Visualisierung vorhanden.")

elif viz_option == "Einzelne Unternehmen":
    st.write("Aktienkursverlauf einzelner Unternehmen (normalisiert auf Index = 100):")
    companies = sorted(viz.get_normalized_dataframe()['Unternehmen'].unique())
    
    if companies:
        selected_company = st.selectbox("Unternehmen wählen:", companies)
        
        figures = viz.plot_by_company()
        if selected_company in figures:
            st.plotly_chart(figures[selected_company], width='stretch')
    else:
        st.info("Keine Daten zur Visualisierung vorhanden.")

elif viz_option == "Vergleich am Ereignis-Tag":
    st.write("Aktienkurse zum Zeitpunkt des Ereignisses (Tag 0):")
    fig = viz.plot_comparison_by_date()
    if fig:
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Keine Daten zur Visualisierung vorhanden.")

elif viz_option == "Zusammenfassungsstatistiken":
    st.write("Statistische Kennzahlen für alle Unternehmen:")
    stats = viz.get_summary_statistics()
    if stats is not None and not stats.empty:
        st.dataframe(stats, width='stretch')
        
        # Zusätzliche Insights
        st.subheader("Insights")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            highest = stats.iloc[0]
            st.metric("Höchster Durchschnittskurs", 
                     f"{highest['Unternehmen']}", 
                     f"${highest['Durchschnitt']:.2f}")
        
        with col2:
            lowest = stats.iloc[-1]
            st.metric("Niedrigster Durchschnittskurs", 
                     f"{lowest['Unternehmen']}", 
                     f"${lowest['Durchschnitt']:.2f}")
        
        with col3:
            max_range = stats.loc[stats['Spannweite'].idxmax()]
            st.metric("Größte Kursschwankung", 
                     f"{max_range['Unternehmen']}", 
                     f"${max_range['Spannweite']:.2f}")
    else:
        st.info("Keine Daten zur Visualisierung vorhanden.")

elif viz_option == "Prozentuale Änderung & EPS":
    st.write("Prozentuale Kursänderung und EPS-Vergleich (Actual vs Estimate):")
    fig = viz.plot_percentage_change_with_eps_panel()
    if fig:
        st.plotly_chart(fig, width='stretch')
        
        # Detaillierte EPS-Tabelle
        st.subheader("📊 EPS-Vergleich Details")
        eps_comparison = viz.get_eps_comparison()
        if eps_comparison is not None and not eps_comparison.empty:
            # Formatiere die EPS-Tabelle
            display_eps = eps_comparison.copy()
            display_eps = display_eps.sort_values('EPS Unterschied', ascending=False, na_position='last')
            st.dataframe(display_eps, width='stretch')
        else:
            st.info("Keine EPS-Daten verfügbar.")
    else:
        st.info("Keine Daten zur Visualisierung vorhanden.")

elif viz_option == "Earnings Metriken":
    st.write("Umfassende Earnings-Analyse: EPS Surprise, Price Move (Earnings Reaction) und Guidance:")
    fig = viz.plot_earnings_metrics()
    if fig:
        st.plotly_chart(fig, width='stretch')
        
        # Detaillierte Metriken-Tabelle
        st.subheader("📈 Detaillierte Earnings Metriken")
        earnings_metrics = viz.get_eps_comparison()
        if earnings_metrics is not None and not earnings_metrics.empty:
            # Formatiere für Anzeige
            display_metrics = earnings_metrics.copy()
            display_metrics = display_metrics.sort_values('EPS Surprise (%)', ascending=False, na_position='last')
            
            # Formatiere Zahlen für bessere Lesbarkeit
            numeric_cols = ['EPS Estimate', 'EPS Actual', 'EPS Unterschied', 'EPS Surprise (%)', 'Price Move (%)']
            for col in numeric_cols:
                if col in display_metrics.columns:
                    display_metrics[col] = display_metrics[col].apply(
                        lambda x: f"{x:.2f}" if isinstance(x, (int, float)) and not pd.isna(x) else x
                    )
            
            st.dataframe(display_metrics, width='stretch')
            
            # Zusammenfassung
            st.subheader("📊 Zusammenfassung")
            col1, col2, col3, col4 = st.columns(4)
            
            # Positive Surprises
            positive_surprises = earnings_metrics[earnings_metrics['EPS Surprise (%)'] > 0]
            with col1:
                st.metric("Positive Surprises", len(positive_surprises))
            
            # Negative Surprises
            negative_surprises = earnings_metrics[earnings_metrics['EPS Surprise (%)'] < 0]
            with col2:
                st.metric("Negative Surprises", len(negative_surprises))
            
            # Positive Price Moves
            positive_moves = earnings_metrics[earnings_metrics['Price Move (%)'] > 0]
            with col3:
                st.metric("Positive Price Moves", len(positive_moves))
            
            # Guidance Positiv
            positive_guidance = earnings_metrics[earnings_metrics['Guidance'] == 'Positiv ✓']
            with col4:
                st.metric("Positive Guidance", len(positive_guidance))
        else:
            st.info("Keine Earnings Metriken verfügbar.")
    else:
        st.info("Keine Daten zur Visualisierung vorhanden.")