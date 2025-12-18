import cdsapi
import os
import zipfile
import xarray as xr
import pandas as pd
from datetime import date, timedelta
import shutil


def procedd(output_dir):

    os.makedirs(output_dir, exist_ok=True)
    try:   
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



year_list = ["2023"]
month_list = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

for year in year_list:
    for month in month_list:
        dir = os.path.join("raw_data", f"{year}_{month}")
        print(f"--- ЗАПУСК ETL ПРОЦЕССА: {year}_{month} ---")

        procedd(dir)

