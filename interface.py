import streamlit as st
from api import get_stock_open_close
from datetime import datetime

st.title("Aktienkurs Abfrage")

st.write("Gib das Tickersymbol und das Datum ein, um Open/Close-Kurse zu erhalten.")

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
