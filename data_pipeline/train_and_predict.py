# File: update_daily_data.py
# Этот скрипт предназначен для ежедневного запуска.
# Он скачивает последние доступные данные ERA5, пропущенные за предыдущие дни.

import cdsapi
import os
from datetime import date, timedelta
from pathlib import Path
import glob # Библиотека для поиска файлов по шаблону
import xarray as xr



# --- Настройки ---
# Директория для сохранения данных
OUTPUT_DIR = './data/'
# Горизонт прогнозирования в часах
N_STEPS_AHEAD = 10

# Параметры запроса
AREA = [44.5, 68.0, 38.0, 82.0] # [Север, Запад, Юг, Восток]
GRID = [0.25, 0.25]
VARIABLES = [
    '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
    '2m_temperature', 'mean_sea_level_pressure', 'total_cloud_cover',
    'total_precipitation',
    'maximum_2m_temperature_since_previous_post_processing',
    'minimum_2m_temperature_since_previous_post_processing',
    'mean_surface_downward_short_wave_radiation_flux',
]
# --- Конец настроек ---


def download_single_day(target_date: date):
    """
    Формирует запрос и скачивает данные за один конкретный день.
    """
    # Формируем имя файла, например: ./data/era5_2024-02-25.nc
    target_filename = f"era5_{target_date.strftime('%Y-%m-%d')}.nc"
    target_filepath = Path(OUTPUT_DIR) / target_filename
    
    # --- КЛЮЧЕВАЯ ПРОВЕРКА: Если файл уже существует, ничего не делаем ---
    if target_filepath.exists():
        print(f"Файл {target_filename} уже существует. Пропускаем.")
        return

    print(f"Обнаружен пропущенный день: {target_date}. Начинаю загрузку в файл {target_filename}...")
    
    try:
        c = cdsapi.Client()
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': VARIABLES,
                'year': str(target_date.year),
                'month': f'{target_date.month:02d}',
                'day': f'{target_date.day:02d}',
                'time': [f'{h:02d}:00' for h in range(24)],
                'area': AREA,
                'grid': GRID,
            },
            str(target_filepath) # API требует путь в виде строки
        )
        print(f"Успешно скачан файл: {target_filename}")
    except Exception as e:
        print(f"!!! Произошла ошибка при скачивании данных за {target_date}: {e}")
        # Не удаляем файл, если он был частично создан, чтобы не пытаться скачать его снова
        # при следующей ошибке. Можно добавить логику для удаления неполных файлов.

def main():
    # 1. Загрузка и объединение данных
    print("Поиск и загрузка всех NetCDF файлов из папки 'data'...")
    # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
    # Находим все файлы, заканчивающиеся на .nc в указанной директории
    nc_files = glob.glob(f'{OUTPUT_DIR}/*.nc')
    if not nc_files:
        print("!!! В папке 'data' не найдено NetCDF файлов. Запустите сначала скрипт 'update_daily_data.py'.")
        return
        
    print(f"Найдено {len(nc_files)} файлов. Объединение...")
    # Используем xr.open_mfdataset для умного объединения множества файлов по времени
    combined_ds = xr.open_mfdataset(nc_files, combine='by_coords')
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    print("Данные успешно объединены.")


if __name__ == '__main__':
    main()