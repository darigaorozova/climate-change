-- Расширенная схема DWH для климатических данных

-- Измерение: Время
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    month INT,
    day INT,
    quarter INT,
    season VARCHAR(10),
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение: Локация
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    country VARCHAR(100),
    region VARCHAR(100),
    continent VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение: Источник данных
CREATE TABLE IF NOT EXISTS dim_source (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(50),
    api_url TEXT,
    refresh_frequency VARCHAR(20),
    last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение: Тип климатического события
CREATE TABLE IF NOT EXISTS dim_event_type (
    event_type_id SERIAL PRIMARY KEY,
    event_name VARCHAR(100) NOT NULL,
    event_category VARCHAR(50),
    severity_level VARCHAR(20),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Факт: Климатические данные
CREATE TABLE IF NOT EXISTS fact_climate (
    fact_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    location_id INT REFERENCES dim_location(location_id),
    source_id INT REFERENCES dim_source(source_id),
    
    -- Климатические метрики
    avg_temp FLOAT,
    min_temp FLOAT,
    max_temp FLOAT,
    precipitation FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    
    -- Выбросы
    co2_level FLOAT,
    ch4_level FLOAT,
    n2o_level FLOAT,
    
    -- Прогнозы
    prediction_temp FLOAT,
    prediction_precipitation FLOAT,
    prediction_co2 FLOAT,
    
    -- Метаданные
    data_quality_score FLOAT,
    is_anomaly BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Факт: Экстремальные события
CREATE TABLE IF NOT EXISTS fact_extreme_events (
    event_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    location_id INT REFERENCES dim_location(location_id),
    event_type_id INT REFERENCES dim_event_type(event_type_id),
    source_id INT REFERENCES dim_source(source_id),
    
    event_count INT DEFAULT 0,
    event_magnitude FLOAT,
    damage_estimate FLOAT,
    fatalities INT,
    
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_fact_climate_date ON fact_climate(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_climate_location ON fact_climate(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_climate_source ON fact_climate(source_id);
CREATE INDEX IF NOT EXISTS idx_fact_climate_year ON fact_climate(date_id);

CREATE INDEX IF NOT EXISTS idx_fact_events_date ON fact_extreme_events(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_location ON fact_extreme_events(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_type ON fact_extreme_events(event_type_id);

-- Заполнение справочных таблиц
INSERT INTO dim_source (source_name, source_type, refresh_frequency) VALUES
('NOAA Climate Data', 'API', 'daily'),
('Berkeley Earth', 'API', 'monthly'),
('NASA GISTEMP', 'API', 'monthly'),
('OpenWeatherMap', 'API', 'hourly'),
('Global Temperature Dataset', 'CSV', 'monthly'),
('CO2 Fossil Global', 'CSV', 'monthly')
ON CONFLICT DO NOTHING;

INSERT INTO dim_location (country, region, continent) VALUES
('Global', 'Global', 'Global'),
('USA', 'North America', 'North America'),
('Russia', 'Europe/Asia', 'Eurasia'),
('China', 'Asia', 'Asia'),
('India', 'Asia', 'Asia'),
('Brazil', 'South America', 'South America'),
('Australia', 'Oceania', 'Oceania'),
('Germany', 'Europe', 'Europe'),
('France', 'Europe', 'Europe'),
('UK', 'Europe', 'Europe')
ON CONFLICT DO NOTHING;

INSERT INTO dim_event_type (event_name, event_category, severity_level) VALUES
('Heat Wave', 'Temperature', 'High'),
('Cold Wave', 'Temperature', 'High'),
('Flood', 'Precipitation', 'High'),
('Drought', 'Precipitation', 'High'),
('Hurricane', 'Wind', 'Extreme'),
('Tornado', 'Wind', 'Extreme'),
('Wildfire', 'Temperature', 'High'),
('Heavy Snow', 'Precipitation', 'Medium')
ON CONFLICT DO NOTHING;

