CREATE TABLE IF NOT EXISTS weather_capitals (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    city VARCHAR(100),
    temperature NUMERIC (5,2),
    feels_like_temp NUMERIC (5,2),
    humidity NUMERIC (3,0),
    wind_speed NUMERIC (5,2),
    description VARCHAR(100),
    icon_url VARCHAR(100),
    longitude NUMERIC (8,6),
    latitude NUMERIC (8,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);