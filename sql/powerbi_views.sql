-- powerbi_views.sql
-- Views optimized for Power BI Dashboard KPI generation

CREATE SCHEMA IF NOT EXISTS dw;

-- 1. Congestion Index View
CREATE OR REPLACE VIEW dw.v_congestion_analysis AS
SELECT 
    s.sensor_id,
    s.location_name,
    s.latitude,
    s.longitude,
    d.date,
    d.year,
    d.month_name,
    d.day_name,
    d.is_weekend,
    t.hour,
    t.minute,
    tr.speed,
    tr.vehicle_volume,
    tr.occupancy,
    tr.congestion_index,
    CASE 
        WHEN tr.congestion_index >= 0.8 THEN 'Critical'
        WHEN tr.congestion_index >= 0.5 THEN 'Heavy'
        WHEN tr.congestion_index >= 0.3 THEN 'Moderate'
        ELSE 'Low'
    END AS congestion_level
FROM dw.fact_traffic_readings tr
JOIN dw.dim_sensors s ON tr.sensor_key = s.sensor_key
JOIN dw.dim_date d ON tr.date_key = d.date_key
JOIN dw.dim_time t ON tr.time_key = t.time_key;

-- 2. Average Travel Time / Speed View
CREATE OR REPLACE VIEW dw.v_travel_time_trends AS
SELECT 
    v.vehicle_id,
    v.vehicle_type,
    v.assigned_route_id,
    d.date,
    t.hour,
    AVG(p.speed) AS avg_speed_mph,
    -- Estimate travel time index: 60 / avg_speed if speed > 0 (minutes per mile)
    CASE 
        WHEN AVG(p.speed) > 0 THEN 60.0 / AVG(p.speed)
        ELSE 0.0
    END AS estimated_minutes_per_mile
FROM dw.fact_gps_pings p
JOIN dw.dim_vehicles v ON p.vehicle_key = v.vehicle_key
JOIN dw.dim_date d ON p.date_key = d.date_key
JOIN dw.dim_time t ON p.time_key = t.time_key
GROUP BY v.vehicle_id, v.vehicle_type, v.assigned_route_id, d.date, t.hour;

-- 3. Accident & Incident Hotspots View
CREATE OR REPLACE VIEW dw.v_incident_hotspots AS
SELECT 
    i.incident_type,
    i.severity,
    i.location_name,
    i.latitude,
    i.longitude,
    d.date,
    t.hour,
    COUNT(*) AS incident_count,
    CASE 
        WHEN i.severity = 'CRITICAL' THEN 3
        WHEN i.severity = 'MAJOR' THEN 2
        ELSE 1
    END AS hazard_score
FROM dw.fact_incident_reports i
JOIN dw.dim_date d ON i.date_key = d.date_key
JOIN dw.dim_time t ON i.time_key = t.time_key
GROUP BY i.incident_type, i.severity, i.location_name, i.latitude, i.longitude, d.date, t.hour;

-- 4. Bus Punctuality View (Calculates delays based on average actual interval vs scheduled)
CREATE OR REPLACE VIEW dw.v_bus_punctuality AS
WITH route_pings AS (
    SELECT 
        v.assigned_route_id,
        p.record_timestamp,
        LEAD(p.record_timestamp) OVER(PARTITION BY v.vehicle_id ORDER BY p.record_timestamp) AS next_ping_time
    FROM dw.fact_gps_pings p
    JOIN dw.dim_vehicles v ON p.vehicle_key = v.vehicle_key
    WHERE v.vehicle_type = 'BUS' AND v.assigned_route_id IS NOT NULL
),
actual_intervals AS (
    SELECT 
        assigned_route_id,
        EXTRACT(EPOCH FROM (next_ping_time - record_timestamp))/60.0 AS gap_minutes
    FROM route_pings
    WHERE next_ping_time IS NOT NULL
)
SELECT 
    ai.assigned_route_id,
    AVG(ai.gap_minutes) AS avg_actual_gap_minutes,
    -- Simple delay estimate: difference from target 10-minute schedules
    CASE 
        WHEN AVG(ai.gap_minutes) > 12 THEN 'Delayed'
        WHEN AVG(ai.gap_minutes) < 8 THEN 'Early/Clustered'
        ELSE 'On Schedule'
    END AS punctuality_status
FROM actual_intervals ai
GROUP BY ai.assigned_route_id;

-- 5. Peak Hour Congestion Index
CREATE OR REPLACE VIEW dw.v_peak_hour_analysis AS
SELECT 
    CASE 
        WHEN t.hour BETWEEN 7 AND 9 THEN 'AM Peak (07:00-09:00)'
        WHEN t.hour BETWEEN 16 AND 18 THEN 'PM Peak (16:00-18:00)'
        ELSE 'Off-Peak'
    END AS time_period,
    d.day_name,
    AVG(tr.speed) AS avg_speed,
    AVG(tr.vehicle_volume) AS avg_volume,
    AVG(tr.congestion_index) AS avg_congestion_index
FROM dw.fact_traffic_readings tr
JOIN dw.dim_date d ON tr.date_key = d.date_key
JOIN dw.dim_time t ON tr.time_key = t.time_key
GROUP BY 
    CASE 
        WHEN t.hour BETWEEN 7 AND 9 THEN 'AM Peak (07:00-09:00)'
        WHEN t.hour BETWEEN 16 AND 18 THEN 'PM Peak (16:00-18:00)'
        ELSE 'Off-Peak'
    END,
    d.day_name;
