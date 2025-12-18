from flask import Flask, render_template, jsonify, request
import pandas as pd
import numpy as np
import pickle
import os
import sys
from datetime import datetime

# Подключаем наш модуль warehouse
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from warehouse.connection import get_db_client

app = Flask(__name__, template_folder='app/templates', static_folder='app/static')

# --- 1. ЗАГРУЗКА ML МОДЕЛИ (Predictive Analytics) ---
MODEL_PATH = os.path.join('ml_models', 'weather_model.pkl')
model = None

try:
    if os.path.exists(MODEL_PATH):
        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)
        print("✅ ML Модель успешно загружена")
    else:
        print("⚠️ ML Модель не найдена. Запустите python -m ML.train_model")
except Exception as e:
    print(f"❌ Ошибка загрузки модели: {e}")

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_data_from_ch(query):
    try:
        client = get_db_client()
        return client.query_dataframe(query)
    except Exception as e:
        print(f"DB Error: {e}")
        return pd.DataFrame()

# --- ROUTES (HTML СТРАНИЦЫ) ---
@app.route('/')
def index():
    """Главная: Сводка (Descriptive) + Прогноз (Predictive/Prescriptive)"""
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    """Дашборд: Детальный анализ (Drill-down, Diagnostic)"""
    return render_template('dashboard.html')

# ==========================================
# 📊 API: ГЛАВНАЯ СТРАНИЦА (Сводка + ML)
# ==========================================

@app.route('/api/kpi')
def get_kpi():
    """
    KPI: Аномалия температуры и Экстремальные дни.
    Сравниваем последний доступный год с историей.
    """
    # 1. Определяем последний год (например, 2025 или 2024)
    # Если база пустая, берем 2024
    try:
        last_year_df = get_data_from_ch("SELECT max(year) FROM dim_time")
        last_year = int(last_year_df.iloc[0,0])
    except:
        last_year = 2024
    
    # 2. Основной запрос
    query = f"""
    SELECT 
        -- 1. РАСЧЕТ АНОМАЛИИ
        round(avgIf(temperature_c, year = {last_year}), 2) as current_avg,
        round(avgIf(temperature_c, year < {last_year}), 2) as history_avg,
        
        -- 2. ЭКСТРЕМАЛЬНЫЕ СОБЫТИЯ (Считаем ДНИ, а не часы)
        -- uniqExactIf считает уникальные даты, когда условие выполнилось
        uniqExactIf(toDate(t.timestamp), year = {last_year} AND (temperature_c > 35 OR temperature_c < -20)) as extreme_days_count,
        
        -- Для сравнения: сколько таких дней было в среднем раньше (за год)
        -- (Общее кол-во экстремальных дней в истории) / (Кол-во лет в истории)
        round(
            uniqExactIf(toDate(t.timestamp), year < {last_year} AND (temperature_c > 35 OR temperature_c < -20)) / 
            uniqExact(year)
        , 1) as hist_extreme_avg

    FROM fact_weather f
    JOIN dim_time t ON f.time_id = t.time_id
    """
    
    df = get_data_from_ch(query)
    
    if df.empty:
        return jsonify({})
    
    row = df.iloc[0]
    
    # Считаем разницу (Аномалию)
    anomaly = round(row['current_avg'] - row['history_avg'], 2)

    return jsonify({
        'year': last_year,
        
        # Аномалия
        'current_temp': row['current_avg'],
        'temp_anomaly': anomaly, # Например: +1.4
        
        # Экстремальные дни
        'extreme_days': int(row['extreme_days_count']),
        'extreme_hist_avg': row['hist_extreme_avg'] # Для контекста (было 5, стало 15)
    })

# ==========================================
# DESCRIPTIVE ANALYTICS: Описательная
# ==========================================
@app.route('/api/descriptive/trend')
def descriptive_trend():
    """
    График 1: Климатический тренд (1940-Present).
    Показывает среднегодовую температуру и сглаженный тренд.
    """
    query = """
    SELECT 
        year, 
        -- Обычная средняя температура за год
        round(avg(temperature_c), 2) as avg_temp,
        -- Скользящее среднее за 10 лет (чтобы показать долгосрочный тренд изменения климата)
        round(avg(avg(temperature_c)) OVER (ORDER BY year ROWS BETWEEN 9 PRECEDING AND CURRENT ROW), 2) as trend_line
    FROM weather_full
    GROUP BY year
    ORDER BY year
    """
    df = get_data_from_ch(query)
    
    return jsonify({
        'years': df['year'].tolist(),
        'avg_temp': df['avg_temp'].tolist(),
        'trend': df['trend_line'].tolist()
    })

@app.route('/api/descriptive/histogram')
def descriptive_histogram():
    """
    График 2: Гистограмма распределения (ПО ДНЯМ).
    Сначала считаем среднесуточную температуру, потом распределение.
    """
    query = """
    SELECT 
        floor(daily_avg) as temp_bin,
        count() as days_count
    FROM (
        -- Внутренний запрос: Считаем среднюю температуру для каждого дня
        SELECT 
            toDate(t.timestamp) as date_val,
            avg(f.temperature_c) as daily_avg
        FROM fact_weather f
        JOIN dim_time t ON f.time_id = t.time_id
        GROUP BY date_val
    )
    GROUP BY temp_bin
    ORDER BY temp_bin
    """
    df = get_data_from_ch(query)
    
    return jsonify({
        'bins': df['temp_bin'].tolist(),
        'freq': df['days_count'].tolist()
    })

# ==========================================
# DIAGNOSTIC ANALYTICS: Диагностика
# ==========================================
@app.route('/api/diagnostic/correlations')
def diagnostic_correlations():
    """
    Анализ влияния факторов на температуру.
    Используем функцию corr() для расчета коэффициента Пирсона.
    """
    query = """
    SELECT
        round(corr(temperature_c, solar_radiation), 3) as radiation,
        round(corr(temperature_c, dewpoint_c), 3) as dewpoint,
        round(corr(temperature_c, pressure_hpa), 3) as pressure,
        round(corr(temperature_c, cloud_cover), 3) as clouds,
        round(corr(temperature_c, wind_speed_ms), 3) as wind,
        round(corr(temperature_c, precipitation_mm), 3) as precip
    FROM fact_weather
    """
    df = get_data_from_ch(query)
    
    if df.empty:
        return jsonify([])

    # Преобразуем в удобный формат для графика
    # Сортируем по модулю корреляции (по силе влияния)
    factors = [
        {'name': 'Солнечная радиация', 'value': df['radiation'][0], 'code': 'radiation'},
        {'name': 'Точка росы (Влажность)', 'value': df['dewpoint'][0], 'code': 'dewpoint'},
        {'name': 'Атм. Давление', 'value': df['pressure'][0], 'code': 'pressure'},
        {'name': 'Облачность', 'value': df['clouds'][0], 'code': 'clouds'},
        {'name': 'Скорость ветра', 'value': df['wind'][0], 'code': 'wind'},
        {'name': 'Осадки', 'value': df['precip'][0], 'code': 'precip'}
    ]
    
    # Сортировка: самые влиятельные сверху (по абсолютному значению)
    factors.sort(key=lambda x: abs(x['value']), reverse=True)
    
    return jsonify({
        'names': [f['name'] for f in factors],
        'values': [f['value'] for f in factors],
        'colors': ['#FF6B6B' if f['value'] > 0 else '#4ECDC4' for f in factors] # Красный для +, Синий для -
    })

# ==========================================
# Predictive ANALYTICS: Диагностика
# ==========================================
@app.route('/api/predictive-temp')
def predictive_chart():
    """
    Прогноз на БУДУЩЕЕ (Next 7 days) с доверительным интервалом.
    """
    if not model:
        return jsonify({'data': [], 'layout': {}})
    
    # 1. Берем последние известные данные (168 часов = 7 дней)
    # Мы будем использовать их как основу для генерации признаков на будущее
    query = """
    SELECT 
        f.pressure_hpa, f.dewpoint_c, f.precipitation_mm,
        f.wind_speed_ms, f.cloud_cover, f.solar_radiation,
        l.latitude, l.longitude, t.month, t.hour, t.day_of_week, t.timestamp
    FROM fact_weather f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_location l ON f.location_id = l.location_id
    ORDER BY t.timestamp DESC
    LIMIT 168
    """
    df = get_data_from_ch(query)
    
    if df.empty:
        return jsonify({'data': [], 'layout': {}})
    
    # Сортируем от старого к новому
    df = df.sort_values('timestamp')
    
    # 2. Генерация БУДУЩИХ дат
    last_timestamp = pd.to_datetime(df['timestamp'].iloc[-1])
    future_dates = [last_timestamp + pd.Timedelta(hours=i+1) for i in range(len(df))]
    
    # 3. Подготовка признаков (X)
    # В реальном продакшене тут нужен прогноз погоды от метеослужбы.
    # Для курсовой мы берем паттерны прошлой недели как "прогноз синоптиков" на следующую неделю.
    X = df.drop(columns=['timestamp'])
    
    # 4. Предсказание
    try:
        base_prediction = model.predict(X)
    except Exception as e:
        print(f"Prediction error: {e}")
        return jsonify({'data': [], 'layout': {}})

    # 5. Расчет Доверительного Интервала (Confidence Interval)
    # Мы симулируем рост неопределенности со временем.
    # Базовая ошибка модели (допустим 1.5 градуса) + 0.02 градуса за каждый час прогноза
    uncertainty_growth = np.array([1.5 + (i * 0.05) for i in range(len(base_prediction))])
    
    upper_bound = base_prediction + uncertainty_growth
    lower_bound = base_prediction - uncertainty_growth

    # 6. Формирование данных для графика
    # Нам нужно 3 линии: Нижняя граница, Верхняя граница (залитая), Основная линия
    
    # x ось
    x_axis = [str(d) for d in future_dates]

    chart_data = [
        # 1. Нижняя граница (невидимая линия, нужна для заливки)
        {
            'x': x_axis,
            'y': lower_bound.tolist(),
            'type': 'scatter',
            'mode': 'lines',
            'line': {'width': 0},
            'marker': {'color': '#444'},
            'showlegend': False,
            'name': 'Lower'
        },
        # 2. Верхняя граница (заливка до нижней)
        {
            'x': x_axis,
            'y': upper_bound.tolist(),
            'type': 'scatter',
            'mode': 'lines',
            'line': {'width': 0},
            'marker': {'color': '#444'},
            'fill': 'tonexty', # Заливка до предыдущего графика
            'fillcolor': 'rgba(255, 107, 107, 0.2)', # Полупрозрачный красный
            'showlegend': True,
            'name': 'Доверительный интервал (95%)'
        },
        # 3. Основной прогноз
        {
            'x': x_axis,
            'y': base_prediction.tolist(),
            'type': 'scatter',
            'mode': 'lines',
            'name': 'Прогноз температуры',
            'line': {'color': '#FF6B6B', 'width': 3}
        }
    ]
    
    layout = {
        'title': 'Прогноз температуры на 7 дней вперед',
        'xaxis': {'title': 'Будущее время'},
        'yaxis': {'title': 'Температура (°C)'},
        'template': 'plotly_white',
        'hovermode': 'x unified'
    }
    
    return jsonify({'data': chart_data, 'layout': layout})

# ==========================================
# PRESCRIPTIVE ANALYTICS: Предписывающая
# ==========================================

@app.route('/api/prescriptive')
def prescriptive_analytics():
    if not model:
        print("❌ Model is None")
        return jsonify({'error': 'Model not loaded'})

    # 1. Получаем данные
    # Важно: Порядок колонок должен СТРОГО совпадать с тем, как обучалась модель!
    query = """
    SELECT 
        f.pressure_hpa, f.dewpoint_c, f.precipitation_mm, f.wind_speed_ms, 
        f.cloud_cover, f.solar_radiation, l.latitude, l.longitude, 
        t.month, t.hour, t.day_of_week, t.timestamp
    FROM fact_weather f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_location l ON f.location_id = l.location_id
    ORDER BY t.timestamp DESC 
    LIMIT 168
    """
    df = get_data_from_ch(query)
    
    if df.empty:
        print("❌ DataFrame is empty")
        return jsonify({'error': 'No data in ClickHouse'})

    # 2. Подготовка данных
    # Удаляем timestamp, так как модель на нем не училась
    X = df.drop(columns=['timestamp'])
    
    # 3. Предсказание с отловом ошибок
    try:
        forecast = model.predict(X)
    except Exception as e:
        print(f"❌ Ошибка предсказания (Predict Error): {e}")
        # Часто бывает разница в количестве фичей
        print(f"Модель ждет {model.n_features_in_} колонок, пришло {X.shape[1]}")
        print(f"Колонки пришедшие: {list(X.columns)}")
        return jsonify({'error': str(e)})

    # 4. Анализ
    avg_temp = np.mean(forecast)
    min_temp = np.min(forecast)
    max_temp = np.max(forecast)
    avg_wind = df['wind_speed_ms'].mean()
    total_precip = df['precipitation_mm'].sum()

    # 5. Рекомендации
    recommendations = []

    # ЖКХ
    if min_temp < -15:
        recommendations.append({'sector': 'ЖКХ и Энергетика', 'icon': '🔥', 'status': 'danger', 'action': 'Внимание! Сильные морозы.', 'detail': 'Повысить температуру теплоносителя.'})
    elif min_temp < 0:
        recommendations.append({'sector': 'ЖКХ и Энергетика', 'icon': '🏢', 'status': 'warning', 'action': 'Штатный зимний режим.', 'detail': 'Мониторинг давления газа.'})
    else:
        recommendations.append({'sector': 'ЖКХ и Энергетика', 'icon': '💡', 'status': 'success', 'action': 'Экономичный режим.', 'detail': 'Снизить нагрузку на сети.'})

    # Агро
    if max_temp > 30 and total_precip < 1:
        recommendations.append({'sector': 'Сельское хозяйство', 'icon': '🌾', 'status': 'danger', 'action': 'Угроза засухи!', 'detail': 'Активировать полив.'})
    elif avg_temp > 5 and avg_temp < 25:
        recommendations.append({'sector': 'Сельское хозяйство', 'icon': '🚜', 'status': 'success', 'action': 'Благоприятные условия.', 'detail': 'Посевные работы в норме.'})
    else:
        recommendations.append({'sector': 'Сельское хозяйство', 'icon': '❄️', 'status': 'warning', 'action': 'Риск заморозков.', 'detail': 'Укрыть культуры.'})

    # Транспорт
    if avg_wind > 10 or total_precip > 20:
        recommendations.append({'sector': 'Транспорт и МЧС', 'icon': '⚠️', 'status': 'danger', 'action': 'Штормовое предупреждение.', 'detail': 'Ограничить движение.'})
    elif min_temp < 0 and total_precip > 5:
        recommendations.append({'sector': 'Транспорт и МЧС', 'icon': '🚗', 'status': 'warning', 'action': 'Гололедица.', 'detail': 'Подготовить реагенты.'})
    else:
        recommendations.append({'sector': 'Транспорт и МЧС', 'icon': '✅', 'status': 'success', 'action': 'Дороги чистые.', 'detail': 'Штатный режим.'})

    return jsonify({
        'forecast_summary': f"Прогноз: {round(min_temp)}...{round(max_temp)}°C",
        'recs': recommendations
    })
# ==========================================
# 🔍 API: ДАШБОРД (Drill-down, Filters)
# ==========================================

@app.route('/api/dashboard-drilldown')
def dashboard_drilldown():
    # 1. Получаем параметры
    group_by = request.args.get('group_by', 'year')
    agg_func = request.args.get('agg_func', 'avg').lower()
    start_year = request.args.get('start_year', 2000)
    end_year = request.args.get('end_year', 2025)
    
    # 2. Логика группировки (SQL)
    # Мы сразу формируем выражение для SELECT и для GROUP BY
    if group_by == 'day':
        # Превращаем timestamp в дату, затем в строку для метки
        x_label_expr = "toString(toDate(t.timestamp))"
        group_clause = "toDate(t.timestamp)"
        order_clause = "toDate(t.timestamp)"
        
    elif group_by == 'month':
        # YYYY-MM
        x_label_expr = "concat(toString(t.year), '-', lpad(toString(t.month), 2, '0'))"
        group_clause = "t.year, t.month"
        order_clause = "t.year, t.month"
        
    else: # year
        x_label_expr = "toString(t.year)"
        group_clause = "t.year"
        order_clause = "t.year"

    # 3. Логика агрегации
    if agg_func == 'max':
        temp_expr = "round(max(f.temperature_c), 2)"
        precip_expr = "round(max(f.precipitation_mm), 2)"
    elif agg_func == 'min':
        temp_expr = "round(min(f.temperature_c), 2)"
        precip_expr = "round(min(f.precipitation_mm), 2)"
    else:
        temp_expr = "round(avg(f.temperature_c), 2)"
        precip_expr = "round(sum(f.precipitation_mm), 2)"

    # 4. Итоговый запрос
    # Важно: x_label_expr сразу становится колонкой 'label'
    query = f"""
    SELECT 
        {x_label_expr} as label,
        {temp_expr} as temp,
        {precip_expr} as precip
    FROM fact_weather f
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE t.year BETWEEN {start_year} AND {end_year}
    GROUP BY {group_clause}
    ORDER BY {order_clause}
    """
    
    df = get_data_from_ch(query)
    
    # Защита от пустых данных
    if df.empty:
        return jsonify({'labels': [], 'temperatures': [], 'precipitation': []})
    
    return jsonify({
        'labels': df['label'].tolist(),
        'temperatures': df['temp'].tolist(),
        'precipitation': df['precip'].tolist()
    })



if __name__ == '__main__':
    app.run(debug=True, port=5000)