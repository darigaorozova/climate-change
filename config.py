import os

# Database Configuration
POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'climate_dw'),
    'user': os.getenv('POSTGRES_USER', 'climate_user'),
    'password': os.getenv('POSTGRES_PASSWORD', '123456'),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432'))
}

# Cassandra Configuration
CASSANDRA_CONFIG = {
    'hosts': os.getenv('CASSANDRA_HOSTS', '127.0.0.1').split(','),
    'keyspace': os.getenv('CASSANDRA_KEYSPACE', 'climate_ks'),
    'port': int(os.getenv('CASSANDRA_PORT', '9042'))
}

# API Keys (можно добавить через переменные окружения)
NOAA_API_KEY = os.getenv('NOAA_API_KEY', '')
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', '')
NASA_API_KEY = os.getenv('NASA_API_KEY', '')

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

