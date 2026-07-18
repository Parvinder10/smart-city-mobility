-- init_oltp.sql
-- Inits operational database schemas and tables for simulating real-time systems

CREATE SCHEMA IF NOT EXISTS oltp;

-- 1. Traffic Sensors
CREATE TABLE IF NOT EXISTS oltp.sensors (
    sensor_id VARCHAR(50) PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE', -- ACTIVE, INACTIVE, MAINTENANCE
    installation_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Transit Routes (Schedules)
CREATE TABLE IF NOT EXISTS oltp.transit_routes (
    route_id VARCHAR(50) PRIMARY KEY,
    route_name VARCHAR(100) NOT NULL,
    route_type VARCHAR(50) NOT NULL, -- BUS, TRAM, SUBWAY
    agency_name VARCHAR(100) NOT NULL,
    schedule_interval_minutes INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. GPS Vehicles
CREATE TABLE IF NOT EXISTS oltp.vehicles (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    license_plate VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL, -- BUS, EMERGENCY, TAXI, SERVICE
    operator_name VARCHAR(100) NOT NULL,
    assigned_route_id VARCHAR(50) REFERENCES oltp.transit_routes(route_id),
    status VARCHAR(20) DEFAULT 'IN_SERVICE', -- IN_SERVICE, MAINTENANCE, OUT_OF_SERVICE
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Weather Stations
CREATE TABLE IF NOT EXISTS oltp.weather_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed Initial Data
INSERT INTO oltp.sensors (sensor_id, location_name, latitude, longitude, status, installation_date) VALUES
('SNS-001', 'Broadway & 42nd St', 40.75797, -73.98554, 'ACTIVE', '2023-01-15'),
('SNS-002', '5th Ave & 59th St', 40.76435, -73.97297, 'ACTIVE', '2023-03-10'),
('SNS-003', '7th Ave & 34th St', 40.75056, -73.99124, 'MAINTENANCE', '2022-11-20'),
('SNS-004', 'FDR Dr & E 96th St', 40.78166, -73.94052, 'ACTIVE', '2023-06-01'),
('SNS-005', 'West Side Hwy & 14th St', 40.74244, -74.00938, 'ACTIVE', '2023-08-15')
ON CONFLICT (sensor_id) DO NOTHING;

INSERT INTO oltp.transit_routes (route_id, route_name, route_type, agency_name, schedule_interval_minutes) VALUES
('RT-101', 'M15-Select Bus Service', 'BUS', 'MTA New York City Transit', 10),
('RT-102', 'M101-Local Bus', 'BUS', 'MTA New York City Transit', 15),
('RT-103', 'M4-Local Bus', 'BUS', 'MTA New York City Transit', 12),
('RT-201', 'Broadway Line', 'SUBWAY', 'MTA Subway', 5)
ON CONFLICT (route_id) DO NOTHING;

INSERT INTO oltp.vehicles (vehicle_id, license_plate, vehicle_type, operator_name, assigned_route_id, status) VALUES
('VH-001', '123-BUS', 'BUS', 'MTA Bus Corp', 'RT-101', 'IN_SERVICE'),
('VH-002', '456-BUS', 'BUS', 'MTA Bus Corp', 'RT-101', 'IN_SERVICE'),
('VH-003', '789-BUS', 'BUS', 'MTA Bus Corp', 'RT-102', 'IN_SERVICE'),
('VH-004', '321-TAX', 'TAXI', 'Yellow Cab Inc', NULL, 'IN_SERVICE'),
('VH-005', '654-EME', 'EMERGENCY', 'NYPD', NULL, 'IN_SERVICE')
ON CONFLICT (vehicle_id) DO NOTHING;

INSERT INTO oltp.weather_stations (station_id, station_name, latitude, longitude) VALUES
('WTH-CENTRAL', 'Central Park Observatory', 40.78286, -73.96536),
('WTH-JFK', 'JFK Airport Station', 40.64131, -73.77813),
('WTH-LGA', 'LaGuardia Airport Station', 40.77693, -73.87397)
ON CONFLICT (station_id) DO NOTHING;
