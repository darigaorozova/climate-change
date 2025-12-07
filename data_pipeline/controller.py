import subprocess
import sys
import os
import logging
from datetime import datetime

# --- КОНФИГУРАЦИЯ ---
# Определяем корневую папку проекта (родительская папка для папки ETL)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
LOG_FILE_PATH = os.path.join(PROJECT_ROOT, "pipeline.log")


# Путь к лог-файлу (будет лежать в корне проекта)
LOG_FILE = os.path.join(PROJECT_ROOT, "pipeline.log")

# --- НАСТРОЙКА ЛОГГЕРА ---
def setup_logger():
    """Настраивает логгер: вывод в консоль и в файл (UTF-8)"""
    logger = logging.getLogger("ETL_Controller")
    logger.setLevel(logging.INFO)
    
    # Очищаем старые хендлеры, чтобы не дублировать сообщения при перезапуске
    if logger.hasHandlers():
        logger.handlers.clear()

    # 1. Хендлер для ФАЙЛА (encoding='utf-8' решает проблему с Windows)
    file_handler = logging.FileHandler(LOG_FILE_PATH, mode='a', encoding='utf-8')
    file_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # 2. Хендлер для КОНСОЛИ
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger

# Инициализируем логгер
logger = setup_logger()

def run_module(module_name):
    """
    Запускает python модуль через subprocess.
    Аналог команды: python -m ETL.daily_ingest
    """
    logger.info(f"🚀 Запуск модуля: {module_name}...")
    
    try:
        # sys.executable гарантирует, что мы используем тот же python, что и сейчас
        result = subprocess.run(
            [sys.executable, "-m", module_name],
            cwd=PROJECT_ROOT,     # ВАЖНО: Запускаем из корня проекта, чтобы видеть warehouse
            capture_output=True  # Перехватываем вывод, чтобы записать в лог при ошибке
        )
        
        # Декодирование вывода (Safe Decode)
        # Пытаемся декодировать как UTF-8, если не выходит (Windows CP1251/866) - заменяем битые символы
        try:
            stdout_str = result.stdout.decode('utf-8')
            stderr_str = result.stderr.decode('utf-8')
        except UnicodeDecodeError:
            # Fallback для русской винды, если UTF-8 не прошел
            stdout_str = result.stdout.decode('cp866', errors='replace')
            stderr_str = result.stderr.decode('cp866', errors='replace')

        # Если код возврата 0 - значит все ок
        if result.returncode == 0:
            logger.info(f"✅ Модуль {module_name} успешно выполнен.")
            
            return True
        else:
            logger.error(f"❌ Ошибка при выполнении {module_name}!")
            logger.error(f"STDERR:\n{stderr_str.strip()}")
            # Иногда полезная инфо об ошибке бывает и в stdout
            if stdout_str.strip():
                logger.error(f"STDOUT (Last lines):\n{stdout_str.strip()}")
            return False
        
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка запуска subprocess: {e}")
        return False

def main():
    logger.info("="*50)
    logger.info("🏁 ЗАПУСК ЕЖЕДНЕВНОГО ETL ПАЙПЛАЙНА")
    
    # ШАГ 1: Ingestion
    # Обрати внимание: имя файла должно быть точным (daily_ingest или daily_ingestion)
    if not run_module("data_pipeline.daily_ingestion"):
        logger.warning("⛔ Пайплайн остановлен на этапе Ingestion.")
        return

    # ШАГ 2: Spark Processing
    if not run_module("data_pipeline.process_data_spark"):
        logger.warning("⛔ Пайплайн остановлен на этапе Spark Processing.")
        return

    logger.info("✨ ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН")
    logger.info("="*50)

if __name__ == "__main__":
    main()