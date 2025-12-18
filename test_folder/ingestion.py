import cdsapi
import os
import zipfile

client = cdsapi.Client()

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
    "year": ["2021"],
    "month": ["08"],
    "day": [f"{i:02d}" for i in range(1, 32)],
    "time": [f"{i:02d}:00" for i in range(24)],
    "data_format": "netcdf",
    "download_format": "zip",  # Явно просим ZIP
    "area": [44.5, 68, 38, 82] # Ваш регион
}

# Организация папок: raw_data / год_месяц
output_dir = "raw_data/2021_08"
os.makedirs(output_dir, exist_ok=True)
zip_filename = os.path.join(output_dir, "data.zip")

print("Скачивание архива...")
client.retrieve(dataset, request, zip_filename)

print("Распаковка...")
with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
    zip_ref.extractall(output_dir)

# Удаляем zip, чтобы не занимал место (опционально)
os.remove(zip_filename)

print(f"Данные готовы в папке: {output_dir}")
print(os.listdir(output_dir))