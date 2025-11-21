CREATE KEYSPACE IF NOT EXISTS climate_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE climate_ks;

CREATE TABLE IF NOT EXISTS climate_data (
    year int,
    country text,
    co2_level float,
    avg_temp float,
    prediction_co2 float,
    prediction_temp float,
    PRIMARY KEY (year, country)
);

