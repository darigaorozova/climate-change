"""
Flask приложение для анализа климатических данных
Полноценный веб-сайт для курсового проекта
"""
from flask import Flask, render_template, jsonify, request
import psycopg2
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import plotly.graph_objs as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
from ml_models.predictions import predict_temperature, predict_co2, generate_predictions

# Импортируем модули проекта
import sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from config import POSTGRES_CONFIG, PROCESSED_DATA_DIR

app = Flask(__name__, 
            template_folder=os.path.join(BASE_DIR, 'templates'), 
            static_folder=os.path.join(BASE_DIR, 'static'))

def get_db_connection():
    """Получение соединения с БД"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def load_climate_data():
    """Загрузка климатических данных из обработанных файлов"""
    processed_file = os.path.join(PROCESSED_DATA_DIR, "climate_combined.csv")
    if os.path.exists(processed_file):
        return pd.read_csv(processed_file)
    # Fallback на старый файл
    old_file = os.path.join(PROCESSED_DATA_DIR, "climate_clean.csv")
    if os.path.exists(old_file):
        return pd.read_csv(old_file)
    return None

def get_kpi_data():
    """Получение KPI метрик"""
    df = load_climate_data()
    if df is None or df.empty:
        return {
            'avg_temp': 0,
            'total_precipitation': 0,
            'extreme_events': 0,
            'co2_level': 0,
            'year': 2023
        }
    
    latest_year = int(df['Year'].max())
    latest_data = df[df['Year'] == latest_year]
    
    if len(latest_data) > 0:
        latest_row = latest_data.iloc[0]
    else:
        latest_row = df.iloc[-1]
    
    # Загружаем данные об экстремальных событиях
    events_file = os.path.join(PROCESSED_DATA_DIR, "extreme_events_regional.csv")
    total_events = 0
    if os.path.exists(events_file):
        events_df = pd.read_csv(events_file)
        if 'Extreme_Events' in events_df.columns:
            total_events = int(events_df['Extreme_Events'].sum())
    
    return {
        'avg_temp': round(float(latest_row.get('Avg_Temp', latest_row.get('avg_temp', 0))), 2),
        'total_precipitation': round(float(latest_row.get('Total_Precipitation', latest_row.get('Precipitation', latest_row.get('Avg_Precipitation', 0)))), 2),
        'extreme_events': total_events,
        'co2_level': round(float(latest_row.get('CO2_Level', latest_row.get('co2_level', 0))), 2),
        'year': latest_year
    }

@app.route("/")
def index():
    """Главная страница с графиками и KPI"""
    return render_template("index.html")

@app.route("/dashboard")
def dashboard():
    """Интерактивный дашборд"""
    return render_template("dashboard.html")

@app.route("/api/kpi")
def api_kpi():
    """API для получения KPI"""
    return jsonify(get_kpi_data())

@app.route("/api/temperature-trend")
def api_temperature_trend():
    """API для графика изменения температуры"""
    df = load_climate_data()
    if df is None or df.empty:
        return jsonify({"error": "No data available"}), 404
    
    year_col = 'Year' if 'Year' in df.columns else df.columns[0]
    temp_col = 'Avg_Temp' if 'Avg_Temp' in df.columns else 'avg_temp'
    
    if temp_col not in df.columns:
        return jsonify({"error": "Temperature column not found"}), 404
    
    # Создаем график
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df[year_col],
        y=df[temp_col],
        mode='lines+markers',
        name='Средняя температура',
        line=dict(color='#FF6B6B', width=3),
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        title='Изменение средней температуры по годам',
        xaxis_title='Год',
        yaxis_title='Температура (°C)',
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return jsonify(json.loads(fig.to_json()))

@app.route("/api/precipitation-trend")
def api_precipitation_trend():
    """API для графика изменения осадков"""
    df = load_climate_data()
    if df is None or df.empty:
        return jsonify({"error": "No data available"}), 404
    
    year_col = 'Year' if 'Year' in df.columns else df.columns[0]
    precip_col = 'Avg_Precipitation' if 'Avg_Precipitation' in df.columns else 'Total_Precipitation' if 'Total_Precipitation' in df.columns else 'Precipitation'
    
    if precip_col not in df.columns:
        return jsonify({"error": "Precipitation column not found"}), 404
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df[year_col],
        y=df[precip_col],
        mode='lines+markers',
        name='Осадки',
        line=dict(color='#4ECDC4', width=3),
        fill='tozeroy',
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        title='Изменение осадков по годам',
        xaxis_title='Год',
        yaxis_title='Осадки (мм)',
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return jsonify(json.loads(fig.to_json()))

@app.route("/api/extreme-events-bar")
def api_extreme_events_bar():
    """API для столбчатой диаграммы экстремальных событий"""
    events_file = os.path.join(PROCESSED_DATA_DIR, "extreme_events_regional.csv")
    
    if os.path.exists(events_file):
        df = pd.read_csv(events_file)
        if 'Region' in df.columns and 'Extreme_Events' in df.columns:
            # Берем последний год для каждого региона
            latest_year = df['Year'].max() if 'Year' in df.columns else None
            if latest_year:
                latest_df = df[df['Year'] == latest_year]
            else:
                latest_df = df.groupby('Region')['Extreme_Events'].sum().reset_index()
                latest_df.columns = ['Region', 'Extreme_Events']
            
            if 'Extreme_Events' not in latest_df.columns and 'Region' in latest_df.columns:
                latest_df = df.groupby('Region')['Extreme_Events'].sum().reset_index()
            
            fig = go.Figure(data=[
                go.Bar(
                    x=latest_df['Region'],
                    y=latest_df['Extreme_Events'],
                    marker_color='#FFA07A',
                    text=latest_df['Extreme_Events'],
                    textposition='auto'
                )
            ])
            
            fig.update_layout(
                title='Количество экстремальных погодных событий по регионам',
                xaxis_title='Регион',
                yaxis_title='Количество событий',
                template='plotly_white',
                height=400
            )
            
            return jsonify(json.loads(fig.to_json()))
    
    return jsonify({"error": "No data available"}), 404

@app.route("/api/temperature-boxplot")
def api_temperature_boxplot():
    """API для Box Plot температур"""
    df = load_climate_data()
    if df is None or df.empty:
        return jsonify({"error": "No data available"}), 404
    
    temp_col = 'Avg_Temp' if 'Avg_Temp' in df.columns else 'avg_temp'
    if temp_col not in df.columns:
        return jsonify({"error": "Temperature column not found"}), 404
    
    # Создаем box plot
    fig = go.Figure()
    
    fig.add_trace(go.Box(
        y=df[temp_col],
        name='Температура',
        boxmean='sd',
        marker_color='#95E1D3'
    ))
    
    fig.update_layout(
        title='Распределение температур (Box Plot)',
        yaxis_title='Температура (°C)',
        template='plotly_white',
        height=400
    )
    
    return jsonify(json.loads(fig.to_json()))

@app.route("/api/predictive-temp")
def api_predictive_temp():
    """API для графика с прогнозом температуры"""
    df = load_climate_data()
    if df is None or df.empty:
        return jsonify({"error": "No data available"}), 404
    
    year_col = 'Year' if 'Year' in df.columns else df.columns[0]
    temp_col = 'Avg_Temp' if 'Avg_Temp' in df.columns else 'avg_temp'
    
    if temp_col not in df.columns:
        return jsonify({"error": "Temperature column not found"}), 404
    
    try:
        # Генерируем прогноз
        max_year = int(df[year_col].max())
        future_years = list(range(max_year + 1, max_year + 6))
        
        predictions = predict_temperature(future_years)
        
        if predictions:
            fig = go.Figure()
            
            # Фактические данные
            fig.add_trace(go.Scatter(
                x=df[year_col],
                y=df[temp_col],
                mode='lines+markers',
                name='Фактическая температура',
                line=dict(color='#FF6B6B', width=3)
            ))
            
            # Прогноз
            fig.add_trace(go.Scatter(
                x=future_years,
                y=predictions,
                mode='lines+markers',
                name='Прогноз',
                line=dict(color='#4ECDC4', width=3, dash='dash'),
                marker=dict(size=8)
            ))
            
            fig.update_layout(
                title='Температура: фактические данные и прогноз',
                xaxis_title='Год',
                yaxis_title='Температура (°C)',
                hovermode='x unified',
                template='plotly_white',
                height=400
            )
            
            return jsonify(json.loads(fig.to_json()))
    except Exception as e:
        print(f"Prediction error: {e}")
        # Возвращаем график без прогноза
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df[year_col],
            y=df[temp_col],
            mode='lines+markers',
            name='Фактическая температура',
            line=dict(color='#FF6B6B', width=3)
        ))
        fig.update_layout(
            title='Температура (без прогноза)',
            xaxis_title='Год',
            yaxis_title='Температура (°C)',
            template='plotly_white',
            height=400
        )
        return jsonify(json.loads(fig.to_json()))
    
    return jsonify({"error": "Prediction failed"}), 500

@app.route("/api/correlation-scatter")
def api_correlation_scatter():
    """API для диаграммы рассеяния корреляций"""
    df = load_climate_data()
    if df is None or df.empty:
        return jsonify({"error": "No data available"}), 404
    
    # Проверяем наличие необходимых колонок
    temp_col = 'Avg_Temp' if 'Avg_Temp' in df.columns else 'avg_temp'
    co2_col = 'CO2_Level' if 'CO2_Level' in df.columns else 'co2_level'
    precip_col = 'Avg_Precipitation' if 'Avg_Precipitation' in df.columns else 'Precipitation'
    year_col = 'Year' if 'Year' in df.columns else None
    
    if temp_col not in df.columns or co2_col not in df.columns:
        return jsonify({"error": "Required columns missing"}), 404
    
    # Создаем scatter plot
    fig = px.scatter(
        df,
        x=co2_col,
        y=temp_col,
        size=precip_col if precip_col in df.columns else None,
        color=year_col if year_col else None,
        hover_data=[year_col] if year_col else None,
        title='Корреляция: Температура vs CO₂',
        labels={co2_col: 'Уровень CO₂', temp_col: 'Средняя температура (°C)'},
        template='plotly_white',
        height=400
    )
    
    return jsonify(json.loads(fig.to_json()))

@app.route("/api/temperature-map")
def api_temperature_map():
    """API для картограммы температуры"""
    country_file = os.path.join(PROCESSED_DATA_DIR, "country_temperature.csv")
    
    if os.path.exists(country_file):
        df = pd.read_csv(country_file)
    else:
        # Генерируем синтетические данные
        countries = ['USA', 'Russia', 'China', 'India', 'Brazil', 'Australia', 'Canada', 'Germany', 'France', 'UK', 'Japan']
        temps = np.random.uniform(5, 30, len(countries))
        df = pd.DataFrame({'Country': countries, 'Avg_Temperature': temps})
    
    # Создаем картограмму (bar chart с цветовой кодировкой)
    fig = go.Figure(data=[
        go.Bar(
            x=df['Country'],
            y=df['Avg_Temperature'],
            marker=dict(
                color=df['Avg_Temperature'],
                colorscale='RdYlBu_r',
                showscale=True,
                colorbar=dict(title="Температура (°C)")
            ),
            text=df['Avg_Temperature'].round(2),
            textposition='auto'
        )
    ])
    
    fig.update_layout(
        title='Распределение температуры по странам',
        xaxis_title='Страна',
        yaxis_title='Температура (°C)',
        template='plotly_white',
        height=400
    )
    
    return jsonify(json.loads(fig.to_json()))

@app.route("/api/dashboard-data")
def api_dashboard_data():
    """API для дашборда с фильтрами"""
    df = load_climate_data()
    if df is None or df.empty:
        return jsonify({"error": "No data available"}), 404
    
    # Получаем параметры фильтрации
    start_year = request.args.get('start_year', type=int)
    end_year = request.args.get('end_year', type=int)
    
    year_col = 'Year' if 'Year' in df.columns else df.columns[0]
    temp_col = 'Avg_Temp' if 'Avg_Temp' in df.columns else 'avg_temp'
    precip_col = 'Avg_Precipitation' if 'Avg_Precipitation' in df.columns else 'Total_Precipitation' if 'Total_Precipitation' in df.columns else 'Precipitation'
    co2_col = 'CO2_Level' if 'CO2_Level' in df.columns else 'co2_level'
    
    filtered_df = df.copy()
    
    if start_year:
        filtered_df = filtered_df[filtered_df[year_col] >= start_year]
    if end_year:
        filtered_df = filtered_df[filtered_df[year_col] <= end_year]
    
    # Агрегация данных
    result = {
        'years': filtered_df[year_col].tolist(),
        'temperatures': filtered_df[temp_col].tolist() if temp_col in filtered_df.columns else [],
        'precipitation': filtered_df[precip_col].tolist() if precip_col in filtered_df.columns else [],
        'co2_levels': filtered_df[co2_col].tolist() if co2_col in filtered_df.columns else [],
        'summary': {
            'avg_temp': float(filtered_df[temp_col].mean()) if temp_col in filtered_df.columns else 0,
            'total_precip': float(filtered_df[precip_col].sum()) if precip_col in filtered_df.columns else 0,
            'avg_co2': float(filtered_df[co2_col].mean()) if co2_col in filtered_df.columns else 0
        }
    }
    
    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)


