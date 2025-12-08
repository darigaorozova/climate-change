import os

# Название базы данных
DB_NAME = 'weather_db'

# Параметры подключения
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = '' # Впиши пароль, если есть

# Словарь для удобной передачи в клиент
db_config = {
    'host': CLICKHOUSE_HOST,
    'port': CLICKHOUSE_PORT,
    'user': CLICKHOUSE_USER,
    'password': CLICKHOUSE_PASSWORD
}
