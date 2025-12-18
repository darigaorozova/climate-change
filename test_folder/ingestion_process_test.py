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


def load(cl, number):
    dataset = "reanalysis-era5-single-levels"

    # Параметры (точно как в вашем запросе)
    request = {
        "product_type": ["reanalysis"],
        "variable": [
            "10m_u_component_of_wind", "10m_v_component_of_wind",
            "2m_dewpoint_temperature", "2m_temperature",
            "mean_sea_level_pressure", "total_precipitation",
            "maximum_2m_temperature_since_previous_post_processing",
            "minimum_2m_temperature_since_previous_post_processing",
            "mean_surface_downward_short_wave_radiation_flux",
            "total_cloud_cover"
        ],
        "year": ["2023"],
        "month": [number],
        "day": [f"{i:02d}" for i in range(1, 32)],
        "time": [f"{i:02d}:00" for i in range(24)],
        "data_format": "netcdf",
        "download_format": "zip",  # Явно просим ZIP
        "area": [44.5, 68, 38, 82] # Ваш регион
    }

    # Организация папок: raw_data / год_месяц
    output_dir = f"raw_data/2022_{number}"
    os.makedirs(output_dir, exist_ok=True)
    zip_filename = os.path.join(output_dir, "data.zip")

    print("Скачивание архива...")
    cl.retrieve(dataset, request, zip_filename)

    print("Распаковка...")
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

    # Удаляем zip, чтобы не занимал место (опционально)
    os.remove(zip_filename)

    return output_dir

# нужно написать месяцы виде строки
month = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
client = cdsapi.Client()

for i in month:
    folder = load(client, i)

    print(f"Данные готовы в папке: {folder}")
    print(os.listdir(folder))


print('все гуд законил')


