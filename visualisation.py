import sqlite3
import json
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Tuple
import numpy as np
from datetime import datetime


class DataVisualization:
    """Klasse zur Visualisierung von Aktiendaten aus der Datenbank"""
    
    def __init__(self, db_path: str = 'data/Datenbank_test.db'):
        """
        Initialisiert die Visualisierungsklasse
        
        Args:
            db_path: Pfad zur SQLite-Datenbank
        """
        self.db_path = db_path
        self.data = None
        self.load_data()
    
    def load_data(self):
        """Lädt alle Daten aus der Datenbank"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT data FROM json_data")
            rows = cursor.fetchall()
            conn.close()
            
            self.data = []
            for row in rows:
                if row[0]:
                    self.data.append(json.loads(row[0]))
            
            print(f"✓ {len(self.data)} Datensätze geladen")
            return self.data
        except Exception as e:
            print(f"✗ Fehler beim Laden der Daten: {e}")
            self.data = []
            return None
    
    def get_dataframe_for_timepoint(self) -> pd.DataFrame:
        """
        Erstellt einen DataFrame mit Zeitpunkten und Preisen pro Unternehmen
        
        Returns:
            DataFrame mit Spalten: Zeitpunkt, Unternehmen, Preis
        """
        records = []
        
        for item in self.data:
            symbol = item.get('Unternehmen', 'Unknown')
            datum = item.get('Datum', 'Unknown')
            zeitpunkte = item.get('Zeitpunkte', {})
            
            # Handle both dictionary and list formats
            if isinstance(zeitpunkte, dict):
                for time_offset, price in zeitpunkte.items():
                    if price is not None:
                        records.append({
                            'Zeitpunkt': int(time_offset),
                            'Unternehmen': symbol,
                            'Preis': price,
                            'Datum': datum
                        })
            elif isinstance(zeitpunkte, list):
                # If it's a list, assume the index represents the offset (-7 to 7)
                for idx, price in enumerate(zeitpunkte):
                    if price is not None:
                        time_offset = idx - 7  # Assuming list has 15 items (from -7 to 7)
                        records.append({
                            'Zeitpunkt': time_offset,
                            'Unternehmen': symbol,
                            'Preis': price,
                            'Datum': datum
                        })
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('Zeitpunkt')
        
        return df
    
    def get_normalized_dataframe(self) -> pd.DataFrame:
        """
        Erstellt einen DataFrame mit normalisierten Preisen (Index = 100 am Anfangspunkt)
        Dies ermöglicht einfacheren Vergleich von Aktien mit unterschiedlichen Kursniveaus
        
        Returns:
            DataFrame mit Spalten: Zeitpunkt, Unternehmen, Preis_normalisiert
        """
        df = self.get_dataframe_for_timepoint()
        
        if df.empty:
            return df
        
        # Normalisiere Preise pro Unternehmen (Basis = ersten Datenpunkt = 100)
        normalized_records = []
        
        for symbol in df['Unternehmen'].unique():
            company_data = df[df['Unternehmen'] == symbol].sort_values('Zeitpunkt')
            
            if len(company_data) > 0:
                # Basiskurs ist der erste Datenpunkt
                base_price = company_data.iloc[0]['Preis']
                
                for idx, row in company_data.iterrows():
                    if base_price > 0:
                        normalized_price = (row['Preis'] / base_price) * 100
                    else:
                        normalized_price = 100
                    
                    normalized_records.append({
                        'Zeitpunkt': row['Zeitpunkt'],
                        'Unternehmen': symbol,
                        'Preis_normalisiert': normalized_price,
                        'Datum': row['Datum']
                    })
        
        return pd.DataFrame(normalized_records)
    
    def plot_overlayed_prices_matplotlib(self, figsize: Tuple[int, int] = (14, 7)) -> plt.Figure:
        """
        Erstellt eine matplotlib-Grafik mit überlagerten Preisen aller Unternehmen
        
        Args:
            figsize: Größe der Grafik (Breite, Höhe)
        
        Returns:
            matplotlib Figure-Objekt
        """
        df = self.get_dataframe_for_timepoint()
        
        if df.empty:
            print("Keine Daten zur Visualisierung vorhanden")
            return None
        
        fig, ax = plt.subplots(figsize=figsize)
        
        # Gruppiere nach Unternehmen
        for symbol in df['Unternehmen'].unique():
            company_data = df[df['Unternehmen'] == symbol].sort_values('Zeitpunkt')
            ax.plot(company_data['Zeitpunkt'], company_data['Preis'], 
                   marker='o', label=symbol, linewidth=2, markersize=6)
        
        # Formatierung
        ax.set_xlabel('Tage relativ zum Ereignis', fontsize=12, fontweight='bold')
        ax.set_ylabel('Aktienkurs ($)', fontsize=12, fontweight='bold')
        ax.set_title('Aktienkursverläufe - Überlagert', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', ncol=1)
        ax.axvline(x=0, color='red', linestyle='--', alpha=0.5, label='Ereignis-Tag')
        
        plt.tight_layout()
        return fig
    
    def plot_overlayed_prices_plotly(self) -> go.Figure:
        """
        Erstellt eine interaktive Plotly-Grafik mit überlagerten Preisen (normalisiert)
        Normalisierung: Index = 100 am Anfangspunkt für jeden Titel
        
        Returns:
            Plotly Figure-Objekt
        """
        df = self.get_normalized_dataframe()
        
        if df.empty:
            print("Keine Daten zur Visualisierung vorhanden")
            return None
        
        fig = go.Figure()
        
        # Füge eine Linie pro Unternehmen hinzu
        for symbol in sorted(df['Unternehmen'].unique()):
            company_data = df[df['Unternehmen'] == symbol].sort_values('Zeitpunkt')
            
            fig.add_trace(go.Scatter(
                x=company_data['Zeitpunkt'],
                y=company_data['Preis_normalisiert'],
                mode='lines+markers',
                name=symbol,
                hovertemplate='<b>%{fullData.name}</b><br>Tag: %{x}<br>Index: %{y:.2f}<extra></extra>'
            ))
        
        # Vertikale Linie bei Tag 0 (Ereignis)
        fig.add_vline(x=0, line_dash="dash", line_color="red", 
                     annotation_text="Ereignis-Tag", annotation_position="top right")
        
        # Horizontale Linie bei 100 (Baseline)
        fig.add_hline(y=100, line_dash="dot", line_color="gray", 
                     annotation_text="Baseline (100)", annotation_position="left")
        
        # Formatierung
        fig.update_layout(
            title='Aktienkursverläufe - Überlagert (Normalisiert - Index = 100)',
            xaxis_title='Tage relativ zum Ereignis',
            yaxis_title='Normalisierter Index (Basis = 100)',
            hovermode='x unified',
            height=600,
            template='plotly_white'
        )
        
        return fig
    
    def plot_by_company(self) -> Dict[str, go.Figure]:
        """
        Erstellt separate Plotly-Grafiken für jedes Unternehmen (normalisiert)
        
        Returns:
            Dictionary mit Symbol als Key und Plotly Figure als Value
        """
        df = self.get_normalized_dataframe()
        
        if df.empty:
            print("Keine Daten zur Visualisierung vorhanden")
            return None
        
        figures = {}
        
        for symbol in sorted(df['Unternehmen'].unique()):
            company_data = df[df['Unternehmen'] == symbol].sort_values('Zeitpunkt')
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=company_data['Zeitpunkt'],
                y=company_data['Preis_normalisiert'],
                mode='lines+markers',
                name=symbol,
                fill='tozeroy',
                hovertemplate='<b>%{fullData.name}</b><br>Tag: %{x}<br>Index: %{y:.2f}<extra></extra>'
            ))
            
            fig.add_vline(x=0, line_dash="dash", line_color="red", opacity=0.5)
            fig.add_hline(y=100, line_dash="dot", line_color="gray", opacity=0.5)
            
            fig.update_layout(
                title=f'Aktienkursverlauf - {symbol} (Normalisiert)',
                xaxis_title='Tage relativ zum Ereignis',
                yaxis_title='Normalisierter Index (Basis = 100)',
                height=400,
                template='plotly_white'
            )
            
            figures[symbol] = fig
        
        return figures
    
    def plot_comparison_by_date(self) -> go.Figure:
        """
        Erstellt eine Grafik, die Unternehmen nach Datum vergleicht
        
        Returns:
            Plotly Figure-Objekt
        """
        df = self.get_dataframe_for_timepoint()
        
        if df.empty:
            print("Keine Daten zur Visualisierung vorhanden")
            return None
        
        # Filtere nur die Ereignistage (Zeitpunkt = 0)
        event_data = df[df['Zeitpunkt'] == 0].copy()
        event_data = event_data.sort_values('Preis', ascending=False)
        
        fig = px.bar(event_data, x='Unternehmen', y='Preis', 
                    title='Aktienkurse am Ereignis-Tag (Zeitpunkt 0)',
                    labels={'Preis': 'Aktienkurs ($)', 'Unternehmen': 'Unternehmen'},
                    hover_data=['Datum'],
                    color='Preis',
                    color_continuous_scale='Viridis')
        
        fig.update_layout(height=500, showlegend=False)
        
        return fig
    
    def get_summary_statistics(self) -> pd.DataFrame:
        """
        Berechnet Zusammenfassungsstatistiken für jedes Unternehmen
        
        Returns:
            DataFrame mit Statistiken
        """
        df = self.get_dataframe_for_timepoint()
        
        if df.empty:
            return None
        
        stats = []
        
        for symbol in df['Unternehmen'].unique():
            company_data = df[df['Unternehmen'] == symbol]['Preis']
            
            stats.append({
                'Unternehmen': symbol,
                'Min Preis': company_data.min(),
                'Max Preis': company_data.max(),
                'Durchschnitt': company_data.mean(),
                'Std Abw.': company_data.std(),
                'Spannweite': company_data.max() - company_data.min()
            })
        
        return pd.DataFrame(stats).sort_values('Durchschnitt', ascending=False)
    
    def plot_percentage_change_and_eps(self) -> go.Figure:
        """
        Erstellt eine interaktive Grafik mit prozentualen Preisänderungen und EPS-Unterschieden
        
        Returns:
            Plotly Figure-Objekt
        """
        df = self.get_dataframe_for_timepoint()
        
        if df.empty:
            print("Keine Daten zur Visualisierung vorhanden")
            return None
        
        # Berechne prozentuale Änderungen
        change_data = []
        
        for symbol in sorted(df['Unternehmen'].unique()):
            company_data = df[df['Unternehmen'] == symbol].sort_values('Zeitpunkt')
            
            if len(company_data) > 0:
                # Basiskurs ist der Preis am Tag -7 (oder ersten verfügbaren Tag)
                base_price = company_data.iloc[0]['Preis']
                
                for idx, row in company_data.iterrows():
                    if base_price > 0:
                        pct_change = ((row['Preis'] - base_price) / base_price) * 100
                    else:
                        pct_change = 0
                    
                    change_data.append({
                        'Zeitpunkt': row['Zeitpunkt'],
                        'Unternehmen': symbol,
                        'Prozentuale Änderung': pct_change,
                        'Preis': row['Preis']
                    })
        
        change_df = pd.DataFrame(change_data)
        
        # Erstelle Figur mit prozentualem Verlauf
        fig = go.Figure()
        
        for symbol in sorted(change_df['Unternehmen'].unique()):
            company_change = change_df[change_df['Unternehmen'] == symbol].sort_values('Zeitpunkt')
            
            fig.add_trace(go.Scatter(
                x=company_change['Zeitpunkt'],
                y=company_change['Prozentuale Änderung'],
                mode='lines+markers',
                name=symbol,
                hovertemplate='<b>%{fullData.name}</b><br>Tag: %{x}<br>Änderung: %{y:.2f}%<extra></extra>'
            ))
        
        # Vertikale Linie bei Tag 0 (Ereignis)
        fig.add_hline(y=0, line_dash="dot", line_color="gray", annotation_text="Baseline (0%)")
        fig.add_vline(x=0, line_dash="dash", line_color="red", annotation_text="Ereignis-Tag")
        
        # Formatierung
        fig.update_layout(
            title='Prozentuale Kursänderung über Zeit',
            xaxis_title='Tage relativ zum Ereignis',
            yaxis_title='Prozentuale Änderung (%)',
            hovermode='x unified',
            height=600,
            template='plotly_white'
        )
        
        return fig
    
    def get_eps_comparison(self) -> pd.DataFrame:
        """
        Berechnet EPS-Vergleiche (Actual vs Estimate) inkl. Surprise, Price Move und Guidance
        
        Returns:
            DataFrame mit EPS-Daten und erweiterten Metriken
        """
        eps_data = []
        df = self.get_dataframe_for_timepoint()
        
        for item in self.data:
            symbol = item.get('Unternehmen', 'Unknown')
            eps_estimate = item.get('EPS Estimate', 'N/A')
            eps_actual = item.get('EPS Actual', 'N/A')
            datum = item.get('Datum', 'Unknown')
            
            # Berechne EPS Unterschied und Prozentual
            eps_diff = None
            eps_diff_pct = None
            
            if (isinstance(eps_estimate, (int, float)) and isinstance(eps_actual, (int, float)) and 
                eps_estimate != 0):
                eps_diff = eps_actual - eps_estimate
                eps_diff_pct = (eps_diff / abs(eps_estimate)) * 100
            
            # Berechne Price Move (Preisänderung vom Tag -1 zu Tag +1 relativ zum Ereignis)
            price_move_pct = None
            company_prices = df[df['Unternehmen'] == symbol].copy()
            
            if not company_prices.empty:
                # Finde Preise für Tag -1 und Tag +1
                day_minus_1 = company_prices[company_prices['Zeitpunkt'] == -1]
                day_plus_1 = company_prices[company_prices['Zeitpunkt'] == 1]
                
                if not day_minus_1.empty and not day_plus_1.empty:
                    price_before = day_minus_1.iloc[0]['Preis']
                    price_after = day_plus_1.iloc[0]['Preis']
                    
                    if price_before > 0:
                        price_move_pct = ((price_after - price_before) / price_before) * 100
            
            # Bestimme Guidance basierend auf EPS Surprise und Price Move
            guidance = "Neutral"
            if eps_diff_pct is not None and price_move_pct is not None:
                eps_positive = eps_diff_pct > 0
                price_positive = price_move_pct > 0
                
                if eps_positive and price_positive:
                    guidance = "Positiv ✓"
                elif eps_positive and not price_positive:
                    guidance = "Enttäuschung"
                elif not eps_positive and price_positive:
                    guidance = "Überraschend"
                else:
                    guidance = "Negativ ✗"
            elif eps_diff_pct is not None:
                guidance = "Positiv ✓" if eps_diff_pct > 0 else "Negativ ✗"
            
            eps_data.append({
                'Unternehmen': symbol,
                'Datum': datum,
                'EPS Estimate': eps_estimate,
                'EPS Actual': eps_actual,
                'EPS Unterschied': eps_diff,
                'EPS Surprise (%)': eps_diff_pct,
                'Price Move (%)': price_move_pct,
                'Guidance': guidance
            })
        
        return pd.DataFrame(eps_data)
    
    def plot_percentage_change_with_eps_panel(self) -> go.Figure:
        """
        Erstellt eine kombinierte Visualisierung mit prozentualem Kursverlauf und EPS-Daten
        
        Returns:
            Plotly Figure mit Subplots
        """
        import plotly.subplots as sp
        
        df = self.get_dataframe_for_timepoint()
        eps_df = self.get_eps_comparison()
        
        if df.empty:
            print("Keine Daten zur Visualisierung vorhanden")
            return None
        
        # Berechne prozentuale Änderungen
        change_data = []
        
        for symbol in sorted(df['Unternehmen'].unique()):
            company_data = df[df['Unternehmen'] == symbol].sort_values('Zeitpunkt')
            
            if len(company_data) > 0:
                base_price = company_data.iloc[0]['Preis']
                
                for idx, row in company_data.iterrows():
                    if base_price > 0:
                        pct_change = ((row['Preis'] - base_price) / base_price) * 100
                    else:
                        pct_change = 0
                    
                    change_data.append({
                        'Zeitpunkt': row['Zeitpunkt'],
                        'Unternehmen': symbol,
                        'Prozentuale Änderung': pct_change,
                        'Preis': row['Preis']
                    })
        
        change_df = pd.DataFrame(change_data)
        
        # Filtere EPS-Daten (nur mit gültigen Werten)
        eps_valid = eps_df[(eps_df['EPS Unterschied'].notna())].copy()
        
        # Erstelle Subplots
        fig = sp.make_subplots(
            rows=2, cols=1,
            subplot_titles=('Prozentuale Kursänderung über Zeit', 'EPS Unterschied (Actual - Estimate)'),
            row_heights=[0.6, 0.4],
            vertical_spacing=0.12
        )
        
        # Oberer Graph: Prozentuale Änderungen
        colors = px.colors.qualitative.Plotly
        for i, symbol in enumerate(sorted(change_df['Unternehmen'].unique())):
            company_change = change_df[change_df['Unternehmen'] == symbol].sort_values('Zeitpunkt')
            color = colors[i % len(colors)]
            
            fig.add_trace(
                go.Scatter(
                    x=company_change['Zeitpunkt'],
                    y=company_change['Prozentuale Änderung'],
                    mode='lines+markers',
                    name=symbol,
                    line=dict(color=color),
                    hovertemplate='<b>%{fullData.name}</b><br>Tag: %{x}<br>Änderung: %{y:.2f}%<extra></extra>'
                ),
                row=1, col=1
            )
        
        # Unterer Graph: EPS-Vergleich (Balkendiagramm)
        if not eps_valid.empty:
            eps_valid_sorted = eps_valid.sort_values('EPS Unterschied', ascending=False)
            
            colors_eps = ['green' if x > 0 else 'red' for x in eps_valid_sorted['EPS Unterschied']]
            
            fig.add_trace(
                go.Bar(
                    x=eps_valid_sorted['Unternehmen'],
                    y=eps_valid_sorted['EPS Unterschied'],
                    marker=dict(color=colors_eps),
                    name='EPS Unterschied',
                    hovertemplate='<b>%{x}</b><br>EPS Diff: %{y:.4f}<extra></extra>',
                    showlegend=False
                ),
                row=2, col=1
            )
        
        # Vertikale Linie bei Tag 0
        fig.add_vline(x=0, line_dash="dash", line_color="red", row=1, col=1)
        
        # Horizontale Linien bei 0
        fig.add_hline(y=0, line_dash="dot", line_color="gray", row=1, col=1)
        fig.add_hline(y=0, line_dash="dot", line_color="gray", row=2, col=1)
        
        # Update Layout
        fig.update_xaxes(title_text="Tage relativ zum Ereignis", row=1, col=1)
        fig.update_xaxes(title_text="Unternehmen", row=2, col=1)
        
        fig.update_yaxes(title_text="Prozentuale Änderung (%)", row=1, col=1)
        fig.update_yaxes(title_text="EPS Unterschied ($)", row=2, col=1)
        
        fig.update_layout(
            title_text='Prozentuale Kursänderung und EPS-Analyse',
            hovermode='x unified',
            height=800,
            template='plotly_white'
        )
        
        return fig
    
    def plot_earnings_metrics(self) -> go.Figure:
        """
        Erstellt eine Grafik mit EPS Surprise, Price Move und Guidance
        
        Returns:
            Plotly Figure-Objekt mit mehreren Subplots
        """
        import plotly.subplots as sp
        
        eps_df = self.get_eps_comparison()
        
        if eps_df.empty or eps_df[['EPS Surprise (%)', 'Price Move (%)']].isna().all().all():
            print("Keine Daten zur Visualisierung vorhanden")
            return None
        
        # Filter nur Datensätze mit gültigen Metriken
        eps_valid = eps_df.dropna(subset=['EPS Surprise (%)', 'Price Move (%)'])
        
        if eps_valid.empty:
            return None
        
        # Sortiere nach EPS Surprise
        eps_valid_sorted = eps_valid.sort_values('EPS Surprise (%)', ascending=False)
        
        # Erstelle Subplots
        fig = sp.make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'EPS Surprise (%)',
                'Price Move (%) - Earnings Reaction',
                'EPS Surprise vs Price Move (Scatter)',
                'Guidance Kategorien'
            ),
            specs=[
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "pie"}]
            ],
            row_heights=[0.5, 0.5],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )
        
        # 1. EPS Surprise (%) - Balkendiagramm
        colors_surprise = ['green' if x > 0 else 'red' for x in eps_valid_sorted['EPS Surprise (%)']]
        fig.add_trace(
            go.Bar(
                x=eps_valid_sorted['Unternehmen'],
                y=eps_valid_sorted['EPS Surprise (%)'],
                marker=dict(color=colors_surprise),
                name='EPS Surprise',
                hovertemplate='<b>%{x}</b><br>EPS Surprise: %{y:.2f}%<extra></extra>',
                showlegend=False
            ),
            row=1, col=1
        )
        
        # 2. Price Move (%) - Balkendiagramm
        colors_price = ['green' if x > 0 else 'red' for x in eps_valid_sorted['Price Move (%)']]
        fig.add_trace(
            go.Bar(
                x=eps_valid_sorted['Unternehmen'],
                y=eps_valid_sorted['Price Move (%)'],
                marker=dict(color=colors_price),
                name='Price Move',
                hovertemplate='<b>%{x}</b><br>Price Move: %{y:.2f}%<extra></extra>',
                showlegend=False
            ),
            row=1, col=2
        )
        
        # 3. Scatter Plot: EPS Surprise vs Price Move
        fig.add_trace(
            go.Scatter(
                x=eps_valid_sorted['EPS Surprise (%)'],
                y=eps_valid_sorted['Price Move (%)'],
                mode='markers+text',
                text=eps_valid_sorted['Unternehmen'],
                textposition='top center',
                marker=dict(
                    size=10,
                    color=eps_valid_sorted['Price Move (%)'],
                    colorscale='RdYlGn',
                    showscale=False
                ),
                hovertemplate='<b>%{text}</b><br>EPS Surprise: %{x:.2f}%<br>Price Move: %{y:.2f}%<extra></extra>',
                showlegend=False
            ),
            row=2, col=1
        )
        
        # 4. Pie Chart: Guidance Kategorien
        guidance_counts = eps_valid_sorted['Guidance'].value_counts()
        colors_guidance = {
            'Positiv ✓': 'green',
            'Negativ ✗': 'red',
            'Neutral': 'gray',
            'Enttäuschung': 'orange',
            'Überraschend': 'blue'
        }
        pie_colors = [colors_guidance.get(g, 'gray') for g in guidance_counts.index]
        
        fig.add_trace(
            go.Pie(
                labels=guidance_counts.index,
                values=guidance_counts.values,
                marker=dict(colors=pie_colors),
                hovertemplate='<b>%{label}</b><br>Anzahl: %{value}<extra></extra>',
                showlegend=False
            ),
            row=2, col=2
        )
        
        # Update Axes - nur für Traces mit Achsen (keine Linien wegen Pie Chart)
        # Row 1 - Bar Charts
        fig.update_xaxes(title_text="Unternehmen", row=1, col=1)
        fig.update_yaxes(title_text="EPS Surprise (%)", row=1, col=1)
        
        fig.update_xaxes(title_text="Unternehmen", row=1, col=2)
        fig.update_yaxes(title_text="Price Move (%)", row=1, col=2)
        
        # Row 2 Col 1 - Scatter Plot
        fig.update_xaxes(title_text="EPS Surprise (%)", row=2, col=1)
        fig.update_yaxes(title_text="Price Move (%)", row=2, col=1)
        
        # Update Layout
        fig.update_layout(
            title_text='Earnings Metriken: EPS Surprise, Price Move & Guidance',
            height=900,
            template='plotly_white',
            showlegend=False
        )
        
        return fig
def get_visualization(db_path: str = 'data/Datenbank_test.db') -> DataVisualization:
    """Erstellt eine Visualisierungsinstanz"""
    return DataVisualization(db_path)


if __name__ == "__main__":
    # Beispiel: Starten der Visualisierung
    print("Starte Datenvisualisierung...\n")
    
    viz = DataVisualization()
    
    # Zeige Zusammenfassung
    print("\n" + "="*60)
    print("ZUSAMMENFASSUNGSSTATISTIKEN")
    print("="*60)
    stats = viz.get_summary_statistics()
    if stats is not None:
        print(stats.to_string(index=False))
    
    # Erstelle und speichere matplotlib-Grafik
    print("\n" + "="*60)
    print("Erstelle matplotlib-Grafik...")
    print("="*60)
    fig_mpl = viz.plot_overlayed_prices_matplotlib()
    if fig_mpl:
        fig_mpl.savefig('data/aktienkurse_ueberlagert.png', dpi=300, bbox_inches='tight')
        print("✓ Grafik gespeichert: data/aktienkurse_ueberlagert.png")
    
    print("\n✓ Visualisierung abgeschlossen!")
