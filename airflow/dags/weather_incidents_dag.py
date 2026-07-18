# weather_incidents_dag.py
import requests
import psycopg2
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONN_STR = "host=postgres dbname=mobility_dw user=postgres password=postgres"
WEATHER_API_URL = "http://fastapi-app:8000/weather"
INCIDENTS_API_URL = "http://fastapi-app:8000/incidents"

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def record_audit_start(**context):
    conn = psycopg2.connect(DB_CONN_STR)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO dw.etl_audit_log (job_name, status, start_time, execution_date)
            VALUES ('Airflow_Weather_Incidents', 'STARTED', CURRENT_TIMESTAMP, CURRENT_DATE)
            RETURNING audit_id;
        """)
        audit_id = cur.fetchone()[0]
        conn.commit()
        context['ti'].xcom_push(key='audit_id', value=audit_id)
        logger.info(f"Audit record created with ID: {audit_id}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Audit log failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def ingest_weather(**context):
    """Fetches real-time weather from API and loads it incrementally into fact_weather_readings."""
    logger.info(f"Calling weather API: {WEATHER_API_URL}")
    response = requests.get(WEATHER_API_URL)
    if response.status_code != 200:
        raise ValueError(f"Weather API returned status code {response.status_code}")
    
    weather_data = response.json()
    logger.info(f"Fetched {len(weather_data)} weather stations metrics.")
    
    conn = psycopg2.connect(DB_CONN_STR)
    cur = conn.cursor()
    records_written = 0
    try:
        # Resolve station keys and insert weather fact
        for report in weather_data:
            station_id = report["station_id"]
            temp = report["temperature"]
            precip = report["precipitation"]
            wind = report["wind_speed"]
            ts_str = report["record_timestamp"]
            ts = datetime.fromisoformat(ts_str)
            
            # Form date and time keys
            date_key = int(ts.strftime("%Y%m%d"))
            time_key = int(ts.strftime("%H%M"))
            
            # Get station key, insert station first if not exists (SCD Type 1)
            cur.execute("""
                INSERT INTO dw.dim_weather_stations (station_id, station_name, latitude, longitude)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (station_id) DO UPDATE SET station_name = EXCLUDED.station_name
                RETURNING station_key;
            """, (station_id, f"{station_id} Station", 40.7, -73.9))
            
            station_key = cur.fetchone()[0]
            
            # Insert into fact weather readings
            cur.execute("""
                INSERT INTO dw.fact_weather_readings (station_key, date_key, time_key, temperature, precipitation, wind_speed, record_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (station_key, date_key, time_key, temp, precip, wind, ts))
            records_written += 1
            
        conn.commit()
        context['ti'].xcom_push(key='weather_records', value=records_written)
        logger.info("Successfully loaded weather records.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Weather ingestion failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def ingest_incidents(**context):
    """Fetches real-time incidents from API and loads them incrementally into fact_incident_reports."""
    logger.info(f"Calling incident API: {INCIDENTS_API_URL}")
    response = requests.get(INCIDENTS_API_URL)
    if response.status_code != 200:
        raise ValueError(f"Incidents API returned status code {response.status_code}")
        
    incidents_data = response.json()
    logger.info(f"Fetched {len(incidents_data)} active incident records.")
    
    conn = psycopg2.connect(DB_CONN_STR)
    cur = conn.cursor()
    records_written = 0
    try:
        for incident in incidents_data:
            inc_type = incident["incident_type"]
            severity = incident["severity"]
            location = incident["location_name"]
            lat = incident["latitude"]
            lon = incident["longitude"]
            ts_str = incident["record_timestamp"]
            ts = datetime.fromisoformat(ts_str)
            
            date_key = int(ts.strftime("%Y%m%d"))
            time_key = int(ts.strftime("%H%M"))
            
            # De-duplicate check (Incremental CDC)
            # If an incident reports at the exact same location and time, skip it
            cur.execute("""
                SELECT COUNT(*) FROM dw.fact_incident_reports 
                WHERE location_name = %s AND record_timestamp = %s;
            """, (location, ts))
            if cur.fetchone()[0] > 0:
                logger.info(f"Skipping duplicate incident at {location} at {ts_str}")
                continue
                
            cur.execute("""
                INSERT INTO dw.fact_incident_reports (incident_type, severity, location_name, latitude, longitude, date_key, time_key, record_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (inc_type, severity, location, lat, lon, date_key, time_key, ts))
            records_written += 1
            
        conn.commit()
        context['ti'].xcom_push(key='incident_records', value=records_written)
        logger.info(f"Successfully loaded {records_written} new incident records.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Incidents ingestion failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def record_audit_success(**context):
    audit_id = context['ti'].xcom_pull(key='audit_id', task_ids='record_audit_start')
    w_records = context['ti'].xcom_pull(key='weather_records', task_ids='ingest_weather') or 0
    i_records = context['ti'].xcom_pull(key='incident_records', task_ids='ingest_incidents') or 0
    
    if not audit_id:
        return
        
    conn = psycopg2.connect(DB_CONN_STR)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE dw.etl_audit_log
            SET status = 'SUCCESS', end_time = CURRENT_TIMESTAMP, records_written = %s
            WHERE audit_id = %s;
        """, (w_records + i_records, audit_id))
        conn.commit()
        logger.info("Audit log updated.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Audit update failed: {e}")
    finally:
        cur.close()
        conn.close()

with DAG(
    'weather_incidents_hourly_etl',
    default_args=default_args,
    description='Polls weather and road incidents from API endpoints hourly',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    task_audit_start = PythonOperator(
        task_id='record_audit_start',
        python_callable=record_audit_start,
    )

    task_weather = PythonOperator(
        task_id='ingest_weather',
        python_callable=ingest_weather,
    )

    task_incidents = PythonOperator(
        task_id='ingest_incidents',
        python_callable=ingest_incidents,
    )

    task_audit_success = PythonOperator(
        task_id='record_audit_success',
        python_callable=record_audit_success,
    )

    task_audit_start >> [task_weather, task_incidents] >> task_audit_success
