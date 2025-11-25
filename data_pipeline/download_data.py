# File: download_daily_data.py
# Этот скрипт предназначен для ежедневного запуска.
# Он скачивает последние доступные данные ERA5, которые могли быть пропущены.

import cdsapi
import os
from datetime import date, timedelta
from pathlib import Path

# --- НАСТРОЙКИ ---
# Директория для сохранения сырых данных
OUTPUT_DIR = './data/raw/'
# Задержка в появлении данных ERA5 в днях (обычно 5-7)
DATA_LAG_DAYS = 5
# Сколько дней в прошлом проверять на случай, если скрипт долго не запускался
DAYS_TO_CHECK_BACK = 15 

# --- ПАРАМЕТРЫ ЗАПРОСА (из вашего примера) ---
AREA = [44.5, 68, 38, 82] # [Север, Запад, Юг, Восток]
VARIABLES = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_dewpoint_temperature",
    "2m_temperature",
    "mean_sea_level_pressure",
    "total_precipitation",
    "maximum_2m_temperature_since_previous_post_processing",
    "minimum_2m_temperature_since_previous_post_processing",
    "mean_surface_downward_short_wave_radiation_flux", # Аналог "mean surface downward short wave radiation flux"
    "total_cloud_cover"
]
# --- КОНЕЦ НАСТРОЕК ---


def download_single_day(target_date: date):
    """
    Формирует запрос и скачивает данные за один конкретный день.
    """
    # Имя файла будет отражать дату, например: ./data/raw/era5_2024-03-10.nc
    target_filename = f"era5_{target_date.strftime('%Y-%m-%d')}.nc"
    target_filepath = Path(OUTPUT_DIR) / target_filename
    
    # КЛЮЧЕВАЯ ПРОВЕРКА: Если файл с такой датой уже существует, ничего не делаем.
    if target_filepath.exists():
        print(f"Файл {target_filename} уже существует. Пропускаем.")
        return

    print(f"Обнаружен пропущенный день: {target_date}. Начинаю загрузку в файл {target_filename}...")
    
    try:
        c = cdsapi.Client()
        
        # Формируем запрос на основе нашего шаблона
        request = {
            "product_type": "reanalysis",
            "format": "netcdf", # Мы просим сразу NetCDF, без zip-архива
            "variable": VARIABLES,
            "year": str(target_date.year),
            "month": f'{target_date.month:02d}',
            "day": f'{target_date.day:02d}',
            "time": [f'{h:02d}:00' for h in range(24)],
            "area": AREA,
            "grid": ["0.25", "0.25"] # Стандартное разрешение для ERA5
        }
        
        c.retrieve("reanalysis-era5-single-levels", request, str(target_filepath))
        
        print(f"Успешно скачан файл: {target_filename}")
        
    except Exception as e:
        print(f"!!! Произошла ошибка при скачивании данных за {target_date}: {e}")
        # Если произошла ошибка, мы не создаем пустой файл, 
        # чтобы скрипт попробовал скачать его снова при следующем запуске.

def main():
    """
    Главная функция. Проверяет последние N дней и докачивает недостающие данные.
    """
    print("--- Запуск ежедневного скрипта обновления данных ERA5 ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    today = date.today()
    
    # Проверяем диапазон дат, начиная с самой свежей возможной и двигаясь в прошлое
    for i in range(DATA_LAG_DAYS, DAYS_TO_CHECK_BACK + 1):
        date_to_process = today - timedelta(days=i)
        
        print(f"\nПроверка даты: {date_to_process}...")
        download_single_day(date_to_process)
        
    print("\n--- Проверка завершена. ---")


if __name__ == '__main__':
    main()