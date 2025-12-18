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
    Выгружает данные из ClickHouse для ML.
    Использует агрессивный сэмплинг для оптимизации памяти.
    """
    client = get_db_client()
    
    print("--- ЗАГРУЗКА ДАННЫХ ДЛЯ ML ---")
    
    # ЛОГИКА СЭМПЛИНГА:
    # У вас 12.5 млн строк.
    # Если взять каждую 20-ю строку (% 20 == 0), получим ~620 000 строк.
    # Это идеальный объем для быстрого обучения RandomForest на ноутбуке.
    
    query = '''
    SELECT 
        f.temperature_c,
        f.pressure_hpa,
        f.dewpoint_c,
        f.precipitation_mm,
        f.wind_speed_ms,
        f.cloud_cover,
        f.solar_radiation,
        l.latitude,
        l.longitude,
        t.month,
        t.hour,
        t.day_of_week
    FROM fact_weather f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_location l ON f.location_id = l.location_id    
    
    WHERE t.year = 2025
      AND cityHash64(f.time_id, f.location_id) % 20 == 0  -- БЕРЕМ 5% ДАННЫХ (каждую 20-ю)
      
    ORDER BY t.timestamp
    '''
    
    print("Выполнение SQL запроса (2025 год, выборка ~5%)...")
    try:
        df = client.query_dataframe(query)
        print(f"✅ Загружено строк: {len(df)}")
        
        # Если вдруг даже 5% это много (больше 1млн), предупредим
        if len(df) > 1000000:
            print(f"⚠️ Внимание: {len(df)} строк может занять 2-5 минут на обучение.")
            
        return df
    except Exception as e:
        print(f"❌ Ошибка ClickHouse: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    load_data_from_clickhouse()