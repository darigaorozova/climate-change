import os
import sys
from datetime import date, timedelta
import pandas as pd 
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round, lit, xxhash64, date_format, 
    year, month, dayofmonth, hour, dayofweek, quarter
)
from clickhouse_driver import Client

# Импорт настроек подключения из модуля warehouse
try:
    from config import db_config
except ImportError:
    print("⚠️ Ошибка импорта warehouse.config. Убедитесь, что запускаете скрипт из корня проекта.")
    sys.exit(1)


# --- КОНФИГУРАЦИЯ ---
LAG_DAYS = 5
# target_date = date.today() - timedelta(days=LAG_DAYS)
target_date = date(2025, 12, 1) 

year_str = target_date.strftime("%Y")
month_str = target_date.strftime("%m")
day_str = target_date.strftime("%d")

# Путь к данным
data_dir = os.path.join("raw_data", f"{year_str}-{month_str}-{day_str}")

def process_and_load():
    print(f"--- ЗАПУСК SPARK STAR SCHEMA ETL ДЛЯ {data_dir} ---")

    if not os.path.exists(data_dir):
        print(f"❌ Ошибка: Папка {data_dir} не найдена. Сначала запустите daily_ingest.py")
        return
    
    # 1. Инициализация Spark (Локальный режим)
    spark = SparkSession.builder \
        .appName("WeatherETL") \
        .master("local[*]") \
        .getOrCreate()
    
    # === ВАЖНОЕ ИСПРАВЛЕНИЕ 1: Работаем строго в UTC ===
    # Это уберет сдвиги на 1 час и прыжки дат
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sparkContext.setLogLevel("WARN")

    try:
        # 2. Чтение Parquet файлов
        df_instant = spark.read.parquet(os.path.join(data_dir, "data_stream-oper_stepType-instant.parquet"))
        df_accum = spark.read.parquet(os.path.join(data_dir, "data_stream-oper_stepType-accum.parquet"))
        df_avg = spark.read.parquet(os.path.join(data_dir, "data_stream-oper_stepType-avg.parquet"))
        df_max = spark.read.parquet(os.path.join(data_dir, "data_stream-oper_stepType-max.parquet"))

        print("Файлы успешно прочитаны.")

        # 3. Объединение (JOIN)
        join_keys = ["time", "latitude", "longitude"]
        full_df = df_instant \
            .join(df_accum, on=join_keys, how="inner") \
            .join(df_avg, on=join_keys, how="inner") \
            .join(df_max, on=join_keys, how="inner")

        # 4. Трансформация данных (Feature Engineering)
        print("Трансформация данных...")
        # Генерируем суррогатные ключи для Star Schema
        # time_id: 2025120100 (YYYYMMDDHH)
        # location_id: 64-битный хэш от координат
        processed_df = full_df \
            .withColumn("time_id", date_format(col("time"), "yyyyMMddHH").cast("long")) \
            .withColumn("location_id", xxhash64(col("latitude"), col("longitude"))) \
            .withColumn("temperature_c", round(col("t2m") - 273.15, 2)) \
            .withColumn("dewpoint_c", round(col("d2m") - 273.15, 2)) \
            .withColumn("max_temp_c", round(col("mx2t") - 273.15, 2)) \
            .withColumn("min_temp_c", round(col("mn2t") - 273.15, 2)) \
            .withColumn("pressure_hpa", round(col("msl") / 100, 2)) \
            .withColumn("precipitation_mm", round(col("tp") * 1000, 4)) \
            .withColumn("wind_speed_ms", round((col("u10")**2 + col("v10")**2)**0.5, 2)) \
            .withColumnRenamed("avg_sdswrf", "solar_radiation") \
            .withColumnRenamed("tcc", "cloud_cover")

        # --- 5. РАЗДЕЛЕНИЕ НА ТАБЛИЦЫ (Схема Звезда) ---
        # A. DIM_TIME (Измерение времени)
        dim_time_df = processed_df.select(
            col("time_id"),
            date_format(col("time"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"), # <-- Строка вместо Timestamp
            year(col("time")).alias("year"),
            month(col("time")).alias("month"),
            dayofmonth(col("time")).alias("day"),
            hour(col("time")).alias("hour"),
            # Формула перевода: Spark(1=Sun) -> ISO(7=Sun)
            ((dayofweek(col("time")) + 5) % 7 + 1).alias("day_of_week"),
            quarter(col("time")).alias("quarter")
        ).distinct()

        # B. DIM_LOCATION (Измерение локации)
        dim_location_df = processed_df.select(
            col("location_id"),
            col("latitude"),
            col("longitude")
        ).distinct()

        # C. FACT_WEATHER (Таблица фактов)
        fact_df = processed_df.select(
            "time_id", "location_id",
            "temperature_c", "dewpoint_c",
            "max_temp_c", "min_temp_c",
            "pressure_hpa", "precipitation_mm",
            "wind_speed_ms", "cloud_cover", "solar_radiation"
        )

        # --- 6. ЗАГРУЗКА В CLICKHOUSE ---
        print(f"Подключение к ClickHouse ({db_config['host']}:{db_config['port']})...")
        client = Client(**db_config)

        # Загрузка DIM_LOCATION
        print(f"Загрузка dim_location ({dim_location_df.count()} записей)...")
        client.execute('INSERT INTO weather_db.dim_location VALUES', dim_location_df.toPandas().to_dict('records'))

        # Загрузка DIM_TIME
        print(f"Загрузка dim_time ({dim_time_df.count()} записей)...")

        # Конвертируем в локальный Pandas DataFrame
        pdf_dim_time = dim_time_df.toPandas()
        
        # --- ФИКС ОШИБКИ 'str has no attribute tzinfo' ---
        # Превращаем строковую колонку обратно в datetime, но БЕЗ часового пояса (naive).
        # Это удовлетворяет драйвер (он видит datetime) и сохраняет значение 00:00:00.
        pdf_dim_time['timestamp'] = pd.to_datetime(pdf_dim_time['timestamp'])

        client.execute('INSERT INTO weather_db.dim_time VALUES', pdf_dim_time.to_dict('records'))

        # Загрузка FACT_WEATHER
        print(f"Загрузка fact_weather ({fact_df.count()} записей)...")
        client.execute('INSERT INTO weather_db.fact_weather VALUES', fact_df.toPandas().to_dict('records'))

        print("\n✅ ETL УСПЕШНО ЗАВЕРШЕН!")


    except Exception as e:
        print(f"Ошибка Spark ETL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    process_and_load()