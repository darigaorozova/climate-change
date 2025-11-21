"""
Flask приложение для анализа климатических данных
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

# Импортируем модели
from ml_models import ClimatePredictor

# Получаем базовый путь проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__, 
            template_folder=os.path.join(BASE_DIR, 'app', 'templates'), 
            static_folder=os.path.join(BASE_DIR, 'app', 'static'))

# Конфигурация БД
DB_CONFIG = {
    'dbname': 'climate_dw',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost'
}

def get_db_connection():
    """Получение соединения с БД"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def load_climate_data():
    """Загрузка климатических данных из обработанных файлов"""
    processed_file = os.path.join(BASE_DIR, "data", "processed", "climate_clean.csv")
    if os.path.exists(processed_file):
        return pd.read_csv(processed_file)
    return None

def get_kpi_data():
    """Получение KPI метрик"""
    df = load_climate_data()
    if df is None or df.empty:
        return {
            'avg_temp': 0,
            'total_precipitation': 0,
            'extreme_events': 0,
            'co2_level': 0
        }
    
    latest_year = df['Year'].max()
    latest_data = df[df['Year'] == latest_year].iloc[0] if len(df[df['Year'] == latest_year]) > 0 else df.iloc[-1]
    
    return {
        'avg_temp': round(latest_data.get('avg_temp', 0), 2),
        'total_precipitation': round(latest_data.get('Precipitation', 0), 2),
        'extreme_events': int(latest_data.get('ExtremeEvents', 0)),
        'co2_level': round(latest_data.get('co2_level', 0), 2),
        'year': int(latest_year)
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
    
    if 'Year' not in df.columns or 'avg_temp' not in df.columns:
        return jsonify({"error": "Required columns missing"}), 404
    
    # Создаем график
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['Year'],
        y=df['avg_temp'],
        mode='lines+markers',
        name='Average Temperature',
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
    
    if 'Year' not in df.columns or 'Precipitation' not in df.columns:
        return jsonify({"error": "Required columns missing"}), 404
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['Year'],
        y=df['Precipitation'],
        mode='lines+markers',
        name='Precipitation',
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
    events_file = os.path.join(BASE_DIR, "data", "processed", "extreme_events_regional.csv")
    
    if os.path.exists(events_file):
        df = pd.read_csv(events_file)
        if 'Region' in df.columns and 'ExtremeEvents' in df.columns:
            regional_sum = df.groupby('Region')['ExtremeEvents'].sum().reset_index()
            
            fig = go.Figure(data=[
                go.Bar(
                    x=regional_sum['Region'],
                    y=regional_sum['ExtremeEvents'],
                    marker_color='#FFA07A',
                    text=regional_sum['ExtremeEvents'],
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
    if df is None or df.empty or 'avg_temp' not in df.columns:
        return jsonify({"error": "No data available"}), 404
    
    # Создаем box plot
    fig = go.Figure()
    
    fig.add_trace(go.Box(
        y=df['avg_temp'],
        name='Temperature',
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
    
    # Обучаем модель и получаем прогноз
    predictor = ClimatePredictor()
    
    try:
        predictor.train_temperature_model(df)
        predictions = predictor.predict_temperature(df, years_ahead=5)
        
        if predictions:
            # Генерируем годы для прогноза
            last_year = int(df['Year'].max())
            future_years = list(range(last_year + 1, last_year + 6))
            
            fig = go.Figure()
            
            # Фактические данные
            fig.add_trace(go.Scatter(
                x=df['Year'],
                y=df['avg_temp'],
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
        return jsonify({"error": str(e)}), 500
    
    return jsonify({"error": "Prediction failed"}), 500

@app.route("/api/correlation-scatter")
def api_correlation_scatter():
    """API для диаграммы рассеяния корреляций"""
    df = load_climate_data()
    if df is None or df.empty:
        return jsonify({"error": "No data available"}), 404
    
    # Проверяем наличие необходимых колонок
    required_cols = ['avg_temp', 'co2_level', 'Precipitation']
    available_cols = [col for col in required_cols if col in df.columns]
    
    if len(available_cols) < 2:
        return jsonify({"error": "Insufficient data for correlation"}), 404
    
    # Создаем scatter plot
    if 'avg_temp' in df.columns and 'co2_level' in df.columns:
        fig = px.scatter(
            df,
            x='co2_level',
            y='avg_temp',
            size='Precipitation' if 'Precipitation' in df.columns else None,
            color='Year' if 'Year' in df.columns else None,
            hover_data=['Year'],
            title='Корреляция: Температура vs CO₂',
            labels={'co2_level': 'Уровень CO₂', 'avg_temp': 'Средняя температура (°C)'},
            template='plotly_white',
            height=400
        )
        
        return jsonify(json.loads(fig.to_json()))
    
    return jsonify({"error": "Required columns missing"}), 404

@app.route("/api/temperature-map")
def api_temperature_map():
    """API для картограммы температуры"""
    df = load_climate_data()
    if df is None or df.empty:
        return jsonify({"error": "No data available"}), 404
    
    # Создаем упрощенную картограмму (используем данные по годам как пример)
    # В реальном проекте здесь были бы данные по странам
    
    # Генерируем синтетические данные по странам для демонстрации
    countries = ['USA', 'Russia', 'China', 'India', 'Brazil', 'Australia', 'Canada', 'Germany']
    latest_year = df['Year'].max()
    latest_temp = df[df['Year'] == latest_year]['avg_temp'].iloc[0] if len(df[df['Year'] == latest_year]) > 0 else df['avg_temp'].mean()
    
    country_data = []
    for country in countries:
        # Генерируем температуру на основе базовой с небольшими вариациями
        country_temp = latest_temp + np.random.normal(0, 5)
        country_data.append({
            'Country': country,
            'Temperature': round(country_temp, 2)
        })
    
    country_df = pd.DataFrame(country_data)
    
    # Создаем картограмму (используем bar chart как альтернативу, т.к. для полноценной карты нужны координаты)
    fig = go.Figure(data=[
        go.Bar(
            x=country_df['Country'],
            y=country_df['Temperature'],
            marker=dict(
                color=country_df['Temperature'],
                colorscale='RdYlBu_r',
                showscale=True,
                colorbar=dict(title="Температура (°C)")
            ),
            text=country_df['Temperature'],
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
    
    filtered_df = df.copy()
    
    if start_year:
        filtered_df = filtered_df[filtered_df['Year'] >= start_year]
    if end_year:
        filtered_df = filtered_df[filtered_df['Year'] <= end_year]
    
    # Агрегация данных
    result = {
        'years': filtered_df['Year'].tolist(),
        'temperatures': filtered_df['avg_temp'].tolist() if 'avg_temp' in filtered_df.columns else [],
        'precipitation': filtered_df['Precipitation'].tolist() if 'Precipitation' in filtered_df.columns else [],
        'co2_levels': filtered_df['co2_level'].tolist() if 'co2_level' in filtered_df.columns else [],
        'summary': {
            'avg_temp': float(filtered_df['avg_temp'].mean()) if 'avg_temp' in filtered_df.columns else 0,
            'total_precip': float(filtered_df['Precipitation'].sum()) if 'Precipitation' in filtered_df.columns else 0,
            'avg_co2': float(filtered_df['co2_level'].mean()) if 'co2_level' in filtered_df.columns else 0
        }
    }
    
    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
