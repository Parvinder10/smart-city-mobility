# transit_schedule_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONN_STR = "host=postgres dbname=mobility_dw user=postgres password=postgres"

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
    """Logs the beginning of the ingestion pipeline in the audit table."""
    conn = psycopg2.connect(DB_CONN_STR)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO dw.etl_audit_log (job_name, status, start_time, execution_date)
            VALUES ('Airflow_Transit_Schedule', 'STARTED', CURRENT_TIMESTAMP, CURRENT_DATE)
            RETURNING audit_id;
        """)
        audit_id = cur.fetchone()[0]
        conn.commit()
        # Save audit_id in XCom for end task
        context['ti'].xcom_push(key='audit_id', value=audit_id)
        logger.info(f"Audit record created with ID: {audit_id}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Audit log failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def simulate_schedule_ingestion(**context):
    """Simulates loading public transit routes schedule delta into OLTP."""
    conn = psycopg2.connect(DB_CONN_STR)
    cur = conn.cursor()
    try:
        # Simulate an updated schedule frequency for RT-101 and a new route RT-301
        logger.info("Ingesting transit schedule delta into oltp.transit_routes...")
        cur.execute("""
            INSERT INTO oltp.transit_routes (route_id, route_name, route_type, agency_name, schedule_interval_minutes, updated_at)
            VALUES 
                ('RT-101', 'M15-Select Bus Service', 'BUS', 'MTA New York City Transit', 8, CURRENT_TIMESTAMP),
                ('RT-301', 'Q60-Local Bus', 'BUS', 'MTA Bus Corp', 20, CURRENT_TIMESTAMP)
            ON CONFLICT (route_id) 
            DO UPDATE SET 
                schedule_interval_minutes = EXCLUDED.schedule_interval_minutes,
                updated_at = CURRENT_TIMESTAMP;
        """)
        
        # Also simulate a vehicle status update to trigger SCD Type 2
        cur.execute("""
            INSERT INTO oltp.vehicles (vehicle_id, license_plate, vehicle_type, operator_name, assigned_route_id, status, updated_at)
            VALUES 
                ('VH-002', '456-BUS', 'BUS', 'MTA Bus Corp', 'RT-101', 'MAINTENANCE', CURRENT_TIMESTAMP),
                ('VH-006', '777-BUS', 'BUS', 'MTA Bus Corp', 'RT-301', 'IN_SERVICE', CURRENT_TIMESTAMP)
            ON CONFLICT (vehicle_id) 
            DO UPDATE SET 
                assigned_route_id = EXCLUDED.assigned_route_id,
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP;
        """)
        conn.commit()
        logger.info("Operational DB successfully updated with delta schedules.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed transit ingestion: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def record_audit_success(**context):
    """Updates the audit log status to SUCCESS."""
    audit_id = context['ti'].xcom_pull(key='audit_id', task_ids='record_audit_start')
    if not audit_id:
        logger.warning("No audit_id found in XCom.")
        return
        
    conn = psycopg2.connect(DB_CONN_STR)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE dw.etl_audit_log
            SET status = 'SUCCESS', end_time = CURRENT_TIMESTAMP, records_written = 3
            WHERE audit_id = %s;
        """, (audit_id,))
        conn.commit()
        logger.info("Audit log updated to SUCCESS.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Audit log update failed: {e}")
    finally:
        cur.close()
        conn.close()

with DAG(
    'transit_schedule_ingestion',
    default_args=default_args,
    description='Orchestrates daily public transit schedule delta updates and triggers Spark SCD Type 2',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task_audit_start = PythonOperator(
        task_id='record_audit_start',
        python_callable=record_audit_start,
    )

    task_ingest = PythonOperator(
        task_id='ingest_schedules',
        python_callable=simulate_schedule_ingestion,
    )

    # Submit SCD Type 2 PySpark job using spark-submit client in Spark container
    task_spark_scd = BashOperator(
        task_id='run_spark_scd_job',
        bash_command='docker exec spark-master spark-submit --packages org.postgresql:postgresql:42.6.0 /opt/spark-apps/batch_historical_etl.py',
        retry_delay=timedelta(seconds=30),
        retries=3
    )

    task_audit_success = PythonOperator(
        task_id='record_audit_success',
        python_callable=record_audit_success,
    )

    task_audit_start >> task_ingest >> task_spark_scd >> task_audit_success
