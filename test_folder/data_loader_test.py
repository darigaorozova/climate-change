import os
import sys
from datetime import date
import pandas as pd 
import findspark

# Инициализация Spark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round, xxhash64, date_format, 
    year, month, dayofmonth, hour, dayofweek, quarter
)
from clickhouse_driver import Client

# Импорт настроек подключения из модуля warehouse
# (Предполагается, что скрипт лежит в корне или рядом с папкой warehouse)
try:
    from config import db_config
except ImportError:
    # Попытка добавить текущую директорию в путь, если запуск не из корня
    sys.path.append(os.getcwd())
    try:
        from config import db_config
    except ImportError:
        print("⚠️ Ошибка: Не найден файл warehouse/config.py")
        sys.exit(1)


# --- ФУНКЦИЯ ДЛЯ ПАКЕТНОЙ ВСТАВКИ ---
def insert_in_batches(client, df, table_name, batch_size=50000):
    """
    Читает Spark DataFrame и вставляет в ClickHouse частями.
    Экономит память (избегает OutOfMemoryError).
    """
    print(f"🚀 Начинаю пакетную загрузку в {table_name}...")
    
    # Подсчет общего количества (для прогресс-бара)
    total_rows = df.count()
    print(f"📊 Всего строк для загрузки: {total_rows}")
    
    # Превращаем в итератор (ленивая загрузка)
    iterator = df.toLocalIterator()
    
    batch = []
    count = 0
    batch_counter = 0

    for row in iterator:
        # Преобразуем строку Spark в словарь Python
        batch.append(row.asDict())
        
        # Если накопили достаточно данных — отправляем
        if len(batch) >= batch_size:
            client.execute(f'INSERT INTO {table_name} VALUES', batch)
            count += len(batch)
            batch_counter += 1
            print(f"   -> Пакет {batch_counter}: загружено {count} / {total_rows} ({(count/total_rows)*100:.1f}%)")
            batch = [] # Очищаем список для освобождения памяти

    # Отправляем остатки, если есть
    if batch:
        client.execute(f'INSERT INTO {table_name} VALUES', batch)
        count += len(batch)
        print(f"   -> Финальный пакет: загружено {count} / {total_rows}")

    print(f"✅ Загрузка в {table_name} завершена.")

def process_and_load(output_dir):
    print(f"--- ЗАПУСК SPARK STAR SCHEMA ETL ДЛЯ {output_dir} ---")

    if not os.path.exists(output_dir):
        print(f"❌ Ошибка: Папка {output_dir} не найдена.")
        return
    
    # 1. Инициализация Spark
    spark = SparkSession.builder \
        .appName("WeatherETL_Batch") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    # Работаем строго в UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 2. Чтение Parquet файлов
        print("Чтение файлов...")
        df_instant = spark.read.parquet(os.path.join(output_dir, "data_stream-oper_stepType-instant.parquet"))
        df_accum = spark.read.parquet(os.path.join(output_dir, "data_stream-oper_stepType-accum.parquet"))
        df_avg = spark.read.parquet(os.path.join(output_dir, "data_stream-oper_stepType-avg.parquet"))
        df_max = spark.read.parquet(os.path.join(output_dir, "data_stream-oper_stepType-max.parquet"))

        # 3. Объединение (JOIN)
        join_keys = ["time", "latitude", "longitude"]
        full_df = df_instant \
            .join(df_accum, on=join_keys, how="inner") \
            .join(df_avg, on=join_keys, how="inner") \
            .join(df_max, on=join_keys, how="inner")

        # 4. Трансформация
        print("Трансформация данных...")
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

        # --- 5. РАЗДЕЛЕНИЕ НА ТАБЛИЦЫ ---

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

        # B. DIM_LOCATION
        dim_location_df = processed_df.select(
            col("location_id"),
            col("latitude"),
            col("longitude")
        ).distinct()

        # C. FACT_WEATHER
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

        # 1. Загрузка DIM_LOCATION
        # Локаций мало (1500 шт), можно грузить сразу через Pandas
        print(f"Загрузка dim_location ({dim_location_df.count()} записей)...")
        client.execute('INSERT INTO weather_db.dim_location VALUES', dim_location_df.toPandas().to_dict('records'))

        # 2. Загрузка DIM_TIME
        # Временных меток мало (24 * 31 = 744 шт), грузим через Pandas с фиксом даты
        print(f"Загрузка dim_time ({dim_time_df.count()} записей)...")
        pdf_dim_time = dim_time_df.toPandas()
        pdf_dim_time['timestamp'] = pd.to_datetime(pdf_dim_time['timestamp']) # Фикс для драйвера
        client.execute('INSERT INTO weather_db.dim_time VALUES', pdf_dim_time.to_dict('records'))

        # 3. Загрузка FACT_WEATHER
        # Фактов ОЧЕНЬ много (>1 млн), используем BATCH INSERT
        insert_in_batches(client, fact_df, "weather_db.fact_weather", batch_size=50000)

        print("\n✅ ETL УСПЕШНО ЗАВЕРШЕН!")

    except Exception as e:
        print(f"❌ Ошибка Spark ETL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":

    year_list = ["2022", "2023", "2024", "2025"]
    month_list = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

    for yea in year_list:
        for mont in month_list:
            print(f"--- ЗАПУСК: {yea}_{mont} ---")

            # Путь к папке с данными
            dir = os.path.join("raw_data", f"{yea}_{mont}")

            process_and_load(dir)