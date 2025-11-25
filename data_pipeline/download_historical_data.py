# File: download_historical_data.py
# Этот скрипт предназначен для скачивания больших исторических данных
# за определенные месяцы.

import cdsapi
import os

# --- НАСТРОЙКИ ---
# Директория, куда будут сохраняться сырые данные
OUTPUT_DIR = './data/raw/'
# Год, за который скачиваем данные
TARGET_YEAR = '2024'
# Месяцы для скачивания. Можно указать один или несколько.
# Сейчас настроено на Август и Сентябрь.
MONTHS_TO_DOWNLOAD = ['06', '07']

# --- ПАРАМЕТРЫ ЗАПРОСА (из вашего примера) ---
AREA = [44.5, 68, 38, 82]
VARIABLES = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_dewpoint_temperature",
    "2m_temperature",
    "mean_sea_level_pressure",
    "total_precipitation",
    "maximum_2m_temperature_since_previous_post_processing",
    "minimum_2m_temperature_since_previous_post_processing",
    "mean_surface_downward_short_wave_radiation_flux",
    "total_cloud_cover"
]
# --- КОНЕЦ НАСТРОЕК ---


def main():
    """
    Основная функция для скачивания данных.
    """
    print("--- Запуск скрипта для скачивания исторических данных ERA5 ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Инициализируем клиент API
    c = cdsapi.Client()

    # Формируем имя файла, чтобы оно было понятным, например: era5_2024_08-09.nc
    month_str = "-".join(MONTHS_TO_DOWNLOAD)
    target_filename = f"era5_{TARGET_YEAR}_{month_str}.nc"
    target_filepath = os.path.join(OUTPUT_DIR, target_filename)

    # Проверяем, не скачан ли уже этот файл
    if os.path.exists(target_filepath):
        print(f"Файл {target_filename} уже существует в папке {OUTPUT_DIR}. Загрузка отменена.")
        return

    # Формируем сам запрос
    request = {
        "product_type": ["reanalysis"],
        "variable": VARIABLES,
        "year": [TARGET_YEAR],
        "month": MONTHS_TO_DOWNLOAD,
        "day": [f"{d:02d}" for d in range(1, 32)], # Все возможные дни
        "time": [f"{h:02d}:00" for h in range(24)], # Все часы
        "area": AREA,  
        "data_format": "netcdf",
        "download_format": "unarchived"
    }

    print(f"Формирование запроса на сервере для данных за {TARGET_YEAR}, месяцы: {MONTHS_TO_DOWNLOAD}.")
    print("Это может занять от нескольких минут до часа в зависимости от загруженности сервера.")
    print(f"Результат будет сохранен в файл: {target_filepath}")

    try:
        c.retrieve("reanalysis-era5-single-levels", request, target_filepath)
        print("\n--------------------------------------------------")
        print(f"Файл {target_filename} успешно скачан!")
        print("--------------------------------------------------")
    except Exception as e:
        print(f"\n!!! Произошла ошибка при выполнении запроса: {e}")
        print("Пожалуйста, проверьте ваш API-ключ и статус серверов Copernicus.")

if __name__ == '__main__':
    main()