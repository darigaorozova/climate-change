import cdsapi
import os
import zipfile
import xarray as xr
import pandas as pd
from datetime import date, timedelta
import shutil

# --- КОНФИГУРАЦИЯ ---
LAG_DAYS = 5 
target_date = date.today() - timedelta(days=LAG_DAYS)
# target_date = date(2025, 12, 1) 


year_str = target_date.strftime("%Y")
month_str = target_date.strftime("%m")
day_str = target_date.strftime("%d")

# Папка: raw_data/2024-01-01 (Убедитесь, что весь путь на английском!)
output_dir = os.path.join("raw_data", f"{year_str}-{month_str}-{day_str}")
os.makedirs(output_dir, exist_ok=True)
zip_filename = os.path.join(output_dir, "daily_data.zip")

print(f"--- ЗАПУСК ETL ПРОЦЕССА: {year_str}-{month_str}-{day_str} ---")

# --- 1. СКАЧИВАНИЕ (Ingestion) ---
client = cdsapi.Client()
dataset = "reanalysis-era5-single-levels"

variables = [
    "10m_u_component_of_wind", "10m_v_component_of_wind",
    "2m_dewpoint_temperature", "2m_temperature",
    "mean_sea_level_pressure", "total_precipitation",
    "maximum_2m_temperature_since_previous_post_processing",
    "minimum_2m_temperature_since_previous_post_processing",
    "mean_surface_downward_short_wave_radiation_flux",
    "total_cloud_cover"
]

request = {
    "product_type": ["reanalysis"],
    "variable": variables,
    "year": [year_str],
    "month": [month_str],
    "day": [day_str],
    "time": [f"{i:02d}:00" for i in range(24)],
    "data_format": "netcdf",
    "download_format": "zip",
    "area": [44.5, 68, 38, 82] 
}

try:
    # Проверяем, есть ли уже обработанные файлы
    existing_parquet = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
    if existing_parquet:
        print(f"Данные за {target_date} уже скачаны и конвертированы.")
    else:
        print("Скачивание данных из CDS API...")
        client.retrieve(dataset, request, zip_filename)
        
        print("Распаковка архива...")
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        os.remove(zip_filename)
        
        # --- 2. КОНВЕРТАЦИЯ В PARQUET (Preprocessing) ---
        print("Начинаю конвертацию NetCDF -> Parquet...")
        
        # Перебираем все файлы в целевой папке
        for filename in os.listdir(output_dir):
            if filename.endswith(".nc"):
                # Формируем полные пути к файлам
                full_nc_path = os.path.join(output_dir, filename)
                full_parquet_path = os.path.join(output_dir, filename.replace(".nc", ".parquet"))
                
                try:    
                    # Теперь путь на английском, открываем стандартно через netcdf4
                    ds = xr.open_dataset(full_nc_path, engine="netcdf4")
                    
                    # === ЛОГИКА ОЧИСТКИ ДАННЫХ ===
                    
                    # 1. Проверяем expver (версии данных)
                    if 'expver' in ds.dims:
                        print(f"Объединение версий ERA5T в файле {filename}...")
                        ds = ds.sel(expver=5).combine_first(ds.sel(expver=1))
                    elif 'expver' in ds.coords:
                        # Удаляем лишнюю координату
                        ds = ds.drop_vars('expver')

                    # 2. Удаляем 'number' (номер ансамбля)
                    if 'number' in ds.coords:
                        ds = ds.drop_vars('number')

                    # 3. Конвертация в DataFrame
                    df = ds.to_dataframe().reset_index()

                    # 4. Унификация имени колонки времени (valid_time -> time)
                    if 'valid_time' in df.columns:
                        df = df.rename(columns={'valid_time': 'time'})

                    if 'time' in df.columns:
                        df['time'] = df['time'].astype('datetime64[us]')
                    
                    # Сохраняем в Parquet
                    df.to_parquet(full_parquet_path, index=False)
                    
                    print(f"Конвертирован: {filename} -> {os.path.basename(full_parquet_path)}")
                    
                    # Закрываем и удаляем исходник
                    ds.close()
                    os.remove(full_nc_path)
                    
                except Exception as e:
                    print(f"Ошибка при обработке {filename}: {e}")

        print("Процесс успешно завершен!")

except Exception as e:
    print(f"Критическая ошибка: {e}")