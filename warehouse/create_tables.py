from clickhouse_driver.errors import Error
# Импортируем наши новые методы и конфиг
from warehouse.connection import get_server_client, get_db_client
from config import DB_NAME

def create_db_structure():
    print(f"--- НАСТРОЙКА СХЕМЫ ЗВЕЗДА В CLICKHOUSE ({DB_NAME}) ---")
    
    try:
        # 1. Создание базы данных (используем server_client)
        server_client = get_server_client()
        print(f"Создание базы данных '{DB_NAME}'...")
        server_client.execute(f'CREATE DATABASE IF NOT EXISTS {DB_NAME}')
        
        # 2. Создание таблиц (используем db_client)
        client = get_db_client()

        print("Удаление старых таблиц (DROP)...")
        client.execute('DROP TABLE IF EXISTS weather_full') # Сначала удаляем View
        client.execute('DROP TABLE IF EXISTS fact_weather')
        client.execute('DROP TABLE IF EXISTS dim_time')
        client.execute('DROP TABLE IF EXISTS dim_location')

        # --- A. DIM_TIME ---
        print("Создание таблицы dim_time...")
        # Заменили UInt64 -> Int64 для совместимости со Spark
        client.execute('''
            CREATE TABLE dim_time (
                time_id Int64,          
                timestamp DateTime,
                year UInt16,
                month UInt8,
                day UInt8,
                hour UInt8,
                day_of_week UInt8,
                quarter UInt8
            ) ENGINE = ReplacingMergeTree()
            ORDER BY time_id
        ''')

        # --- B. DIM_LOCATION ---
        print("Создание таблицы dim_location...")
        # Заменили UInt64 -> Int64, так как хэш может быть отрицательным
        client.execute('''
            CREATE TABLE dim_location (
                location_id Int64,      
                latitude Float64,
                longitude Float64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY location_id
        ''')

        # --- C. FACT_WEATHER ---
        print("Создание таблицы fact_weather...")
        client.execute('''
            CREATE TABLE fact_weather (
                time_id Int64,
                location_id Int64,
                temperature_c Float64,
                dewpoint_c Float64,
                max_temp_c Float64,
                min_temp_c Float64,
                pressure_hpa Float64,
                precipitation_mm Float32,
                wind_speed_ms Float64,
                cloud_cover Float32,
                solar_radiation Float32
            ) ENGINE = MergeTree()
            ORDER BY (time_id, location_id)
        ''')
        
        # --- D. VIEW ---
        print("Создание представления weather_full...")
        client.execute('''
            CREATE VIEW weather_full AS
            SELECT 
                f.*,
                t.timestamp, t.year, t.month, t.day, t.hour, t.day_of_week,
                l.latitude, l.longitude
            FROM fact_weather f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_location l ON f.location_id = l.location_id
        ''')

        print("\n✅ Успешно! Таблицы пересозданы с типом Int64.")

    except Error as e:
        print(f"❌ Ошибка ClickHouse: {e}")
    except Exception as e:
        print(f"❌ Общая ошибка: {e}")

if __name__ == "__main__":
    create_db_structure()