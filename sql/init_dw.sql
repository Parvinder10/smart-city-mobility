-- init_dw.sql
-- Initializes the OLAP Data Warehouse Schema

CREATE SCHEMA IF NOT EXISTS dw;

-- 1. Date Dimension
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- 2. Time Dimension
CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_key INT PRIMARY KEY, -- HHMM
    hour INT NOT NULL,
    minute INT NOT NULL
);

-- 3. Sensors Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS dw.dim_sensors (
    sensor_key SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    location_name VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Vehicles Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS dw.dim_vehicles (
    vehicle_key SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL,
    license_plate VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    operator_name VARCHAR(100) NOT NULL,
    assigned_route_id VARCHAR(50),
    status VARCHAR(20) NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Weather Stations Dimension (SCD Type 1)
CREATE TABLE IF NOT EXISTS dw.dim_weather_stations (
    station_key SERIAL PRIMARY KEY,
    station_id VARCHAR(50) UNIQUE NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Fact Traffic Readings (Granular telemetry)
CREATE TABLE IF NOT EXISTS dw.fact_traffic_readings (
    reading_id SERIAL PRIMARY KEY,
    sensor_key INT REFERENCES dw.dim_sensors(sensor_key),
    date_key INT REFERENCES dw.dim_date(date_key),
    time_key INT REFERENCES dw.dim_time(time_key),
    speed DOUBLE PRECISION NOT NULL,
    vehicle_volume INT NOT NULL,
    occupancy DOUBLE PRECISION NOT NULL,
    congestion_index DOUBLE PRECISION NOT NULL,
    record_timestamp TIMESTAMP NOT NULL
);

-- 7. Fact GPS Pings
CREATE TABLE IF NOT EXISTS dw.fact_gps_pings (
    ping_id SERIAL PRIMARY KEY,
    vehicle_key INT REFERENCES dw.dim_vehicles(vehicle_key),
    date_key INT REFERENCES dw.dim_date(date_key),
    time_key INT REFERENCES dw.dim_time(time_key),
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,
    record_timestamp TIMESTAMP NOT NULL
);

-- 8. Fact Weather Readings
CREATE TABLE IF NOT EXISTS dw.fact_weather_readings (
    reading_id SERIAL PRIMARY KEY,
    station_key INT REFERENCES dw.dim_weather_stations(station_key),
    date_key INT REFERENCES dw.dim_date(date_key),
    time_key INT REFERENCES dw.dim_time(time_key),
    temperature DOUBLE PRECISION NOT NULL,
    precipitation DOUBLE PRECISION NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL,
    record_timestamp TIMESTAMP NOT NULL
);

-- 9. Fact Incident Reports
CREATE TABLE IF NOT EXISTS dw.fact_incident_reports (
    incident_id SERIAL PRIMARY KEY,
    incident_type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    location_name VARCHAR(200) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    date_key INT REFERENCES dw.dim_date(date_key),
    time_key INT REFERENCES dw.dim_time(time_key),
    record_timestamp TIMESTAMP NOT NULL
);

-- 10. Audit Log Table
CREATE TABLE IF NOT EXISTS dw.etl_audit_log (
    audit_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL, -- STARTED, SUCCESS, FAILED
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_read INT DEFAULT 0,
    records_written INT DEFAULT 0,
    error_message TEXT,
    execution_date DATE DEFAULT CURRENT_DATE
);

-- 11. Dead-Letter Queue Table
CREATE TABLE IF NOT EXISTS dw.dead_letter_queue (
    dlq_id SERIAL PRIMARY KEY,
    payload TEXT NOT NULL,
    validation_error TEXT NOT NULL,
    source_topic VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --- Seed Dim Date and Dim Time ---
-- Seed Date Dim (from 2026-01-01 to 2026-12-31)
DO $$
DECLARE
    start_dt DATE := '2026-01-01';
    end_dt DATE := '2026-12-31';
    curr_dt DATE := start_dt;
BEGIN
    WHILE curr_dt <= end_dt LOOP
        INSERT INTO dw.dim_date (date_key, date, year, month, month_name, day, day_of_week, day_name, is_weekend)
        VALUES (
            CAST(TO_CHAR(curr_dt, 'YYYYMMDD') AS INT),
            curr_dt,
            CAST(TO_CHAR(curr_dt, 'YYYY') AS INT),
            CAST(TO_CHAR(curr_dt, 'MM') AS INT),
            TO_CHAR(curr_dt, 'Month'),
            CAST(TO_CHAR(curr_dt, 'DD') AS INT),
            CAST(TO_CHAR(curr_dt, 'ID') AS INT), -- 1 (Monday) to 7 (Sunday)
            TO_CHAR(curr_dt, 'Day'),
            CASE WHEN TO_CHAR(curr_dt, 'ID') IN ('6', '7') THEN TRUE ELSE FALSE END
        ) ON CONFLICT (date_key) DO NOTHING;
        curr_dt := curr_dt + INTERVAL '1 day';
    END LOOP;
END $$;

-- Seed Time Dim (every 1 minute)
DO $$
DECLARE
    h INT;
    m INT;
    tkey INT;
BEGIN
    FOR h IN 0..23 LOOP
        FOR m IN 0..59 LOOP
            tkey := h * 100 + m;
            INSERT INTO dw.dim_time (time_key, hour, minute)
            VALUES (tkey, h, m)
            ON CONFLICT (time_key) DO NOTHING;
        END LOOP;
    END LOOP;
END $$;
