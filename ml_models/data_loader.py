import pandas as pd
import sys
import os

# Добавляем корень проекта в путь, чтобы видеть папку warehouse
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from warehouse.connection import get_db_client

def load_data_from_clickhouse():
    """
    Выгружает данные из ClickHouse, объединяя факты с измерениями.
    Возвращает Pandas DataFrame.
    """
    client = get_db_client()
    
    print("--- ЗАГРУЗКА ДАННЫХ ДЛЯ ML ---")
    
    # Мы НЕ берем max_temp_c и min_temp_c как признаки (Features),
    # потому что они почти равны целевой переменной (это будет читерство/Data Leakage).
    # Мы предсказываем temperature_c на основе атмосферных явлений.
    
    query = '''
    SELECT 
        -- Целевая переменная (Target)
        f.temperature_c,
        
        -- Признаки (Features)
        f.pressure_hpa,
        f.dewpoint_c,
        f.precipitation_mm,
        f.wind_speed_ms,
        f.cloud_cover,
        f.solar_radiation,
        
        -- Контекст (из измерений)
        l.latitude,
        l.longitude,
        t.month,
        t.hour,
        t.day_of_week
    FROM fact_weather f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_location l ON f.location_id = l.location_id
    ORDER BY t.timestamp
    '''
    
    print("Выполнение SQL запроса...")
    df = client.query_dataframe(query)
    print(f"✅ Загружено строк: {len(df)}")
    print("Пример данных:")
    print(df.head())
    
    return df

if __name__ == "__main__":
    load_data_from_clickhouse()