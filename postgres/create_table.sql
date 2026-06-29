CREATE TABLE weather (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    average_temperature DOUBLE PRECISION
)