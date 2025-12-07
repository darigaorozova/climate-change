from clickhouse_driver import Client
from config import db_config, DB_NAME

def get_server_client():
    """
    Возвращает клиент для подключения к серверу (без выбора конкретной БД).
    Нужен для создания базы данных.
    """
    return Client(**db_config)

def get_db_client():
    """
    Возвращает клиент, подключенный к нашей базе данных (weather_db).
    Используется для вставки данных и запросов.
    """
    return Client(database=DB_NAME, **db_config)