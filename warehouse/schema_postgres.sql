-- Создание базы данных (выполняется отдельно)
-- CREATE DATABASE climate_dw;

-- Таблица измерения: Дата
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    month INT,
    day INT,
    season VARCHAR(10),
    UNIQUE(year, month, day)
);

-- Таблица измерения: Локация
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    country VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    UNIQUE(country, region)
);

-- Таблица измерения: Источник данных
CREATE TABLE IF NOT EXISTS dim_source (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    api_url TEXT,
    refresh_freq VARCHAR(20)
);

-- Таблица фактов: Климатические данные
CREATE TABLE IF NOT EXISTS fact_climate (
    fact_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    location_id INT REFERENCES dim_location(location_id),
    source_id INT REFERENCES dim_source(source_id),
    co2_level FLOAT,
    avg_temp FLOAT,
    min_temp FLOAT,
    max_temp FLOAT,
    precipitation FLOAT,
    extreme_events INT,
    prediction_co2 FLOAT,
    prediction_temp FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_id, location_id, source_id)
);

-- Индексы для улучшения производительности
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_climate(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_climate(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_source ON fact_climate(source_id);
CREATE INDEX IF NOT EXISTS idx_dim_date_year ON dim_date(year);
