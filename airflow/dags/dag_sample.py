from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CRM_FILE = '/opt/airflow/data/crm_sample.csv'
TELEMETRY_FILE = '/opt/airflow/data/telemetry_sample.csv'

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 1)
}

def extract_crm() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(CRM_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

def extract_telemetry() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(TELEMETRY_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def transform_data_mart(**context):
    import statistics
    from datetime import datetime as dt

    ti = context['ti']
    crm_data = ti.xcom_pull(task_ids='extract_crm_task')
    telemetry_data = ti.xcom_pull(task_ids='extract_telemetry_task')
    
    crm_dict = {}
    for crm in crm_data:
        crm_dict[crm['buyer_id']] = crm
    
    telemetry_agg: Dict[tuple, Dict[str, any]] = {}
    for tel in telemetry_data:
        key = (tel['user_id'], tel['device_id'])
        telemetry_agg.setdefault(key, {
            'user_id': tel['user_id'],
            'device_id': tel['device_id'],
            'battery_levels': [],
            'response_times': [],
            'muscle_signals': [],
            'actuator_powers': [],
            'temperatures': [],
            'usage_durations': [],
            'action_count': 0,
            'last_activity': None,
        })
        entry = telemetry_agg[key]
        entry['battery_levels'].append(float(tel['battery_level']))
        entry['response_times'].append(float(tel['response_time']))
        entry['muscle_signals'].append(float(tel['muscle_signal_strength']))
        entry['actuator_powers'].append(float(tel['actuator_power']))
        entry['temperatures'].append(float(tel['device_temperature']))
        entry['usage_durations'].append(int(tel['usage_duration']))
        entry['action_count'] += 1
        ts = dt.fromisoformat(tel['timestamp'])
        if not entry['last_activity'] or ts > entry['last_activity']:
            entry['last_activity'] = ts

    result = []
    for (user_id, device_id), tel_data in telemetry_agg.items():
        crm_info = crm_dict.get(user_id, {})
        result.append({
            'user_id': user_id,
            'patient_name': crm_info.get('patient_name', 'Unknown'),
            'device_id': device_id,
            'amputation_type': crm_info.get('amputation_type'),
            'prosthesis_type': crm_info.get('prosthesis_type'),
            'order_date': crm_info.get('order_date'),
            'delivery_date': crm_info.get('delivery_date'),
            'status': crm_info.get('status'),
            'training_sessions': int(crm_info.get('training_sessions', 0)),
            'response_time_target': int(crm_info.get('response_time_target', 0)),
            'avg_battery_level': round(statistics.fmean(tel_data['battery_levels']), 2),
            'avg_response_time': round(statistics.fmean(tel_data['response_times']), 2),
            'avg_muscle_signal_strength': round(statistics.fmean(tel_data['muscle_signals']), 3),
            'avg_actuator_power': round(statistics.fmean(tel_data['actuator_powers']), 2),
            'avg_device_temperature': round(statistics.fmean(tel_data['temperatures']), 2),
            'total_usage_duration': sum(tel_data['usage_durations']),
            'action_count': tel_data['action_count'],
            'last_activity_ts': tel_data['last_activity'].isoformat(),
        })
    
    ti.xcom_push(key='data_mart', value=result)


def load_data_mart(**context):
    ti = context['ti']
    data_mart = ti.xcom_pull(task_ids='transform_data_mart_task', key='data_mart') or []
    hook = PostgresHook(postgres_conn_id='airflow_db')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    for row in data_mart:
        cur.execute(
            """
            INSERT INTO data_mart (
                user_id, patient_name, device_id, amputation_type, prosthesis_type,
                order_date, delivery_date, status, training_sessions, response_time_target,
                avg_battery_level, avg_response_time, avg_muscle_signal_strength,
                avg_actuator_power, avg_device_temperature, total_usage_duration,
                action_count, last_activity_ts
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                row['user_id'], row['patient_name'], row['device_id'], 
                row['amputation_type'], row['prosthesis_type'],
                row['order_date'], row['delivery_date'], row['status'],
                row['training_sessions'], row['response_time_target'],
                row['avg_battery_level'], row['avg_response_time'], 
                row['avg_muscle_signal_strength'], row['avg_actuator_power'],
                row['avg_device_temperature'], row['total_usage_duration'],
                row['action_count'], row['last_activity_ts']
            )
        )
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='bionicpro_data_mart_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 2 * * *',
    catchup=False,
    description='ETL процесс для создания витрины данных BionicPRO'
) as dag:

    create_crm_table = PostgresOperator(
        task_id='create_crm_table',
        postgres_conn_id='airflow_db',
        sql="""
        DROP TABLE IF EXISTS crm_data;
        CREATE TABLE crm_data (
            id SERIAL PRIMARY KEY,
            order_number TEXT,
            total NUMERIC(10,2),
            discount NUMERIC(10,2),
            buyer_id TEXT NOT NULL,
            patient_name TEXT,
            amputation_type TEXT,
            prosthesis_type TEXT,
            order_date DATE,
            delivery_date DATE,
            status TEXT,
            training_sessions INT,
            response_time_target INT,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    create_telemetry_table = PostgresOperator(
        task_id='create_telemetry_table',
        postgres_conn_id='airflow_db',
        sql="""
        DROP TABLE IF EXISTS telemetry_data;
        CREATE TABLE telemetry_data (
            id SERIAL PRIMARY KEY,
            device_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            timestamp TIMESTAMPTZ,
            battery_level NUMERIC(5,2),
            response_time NUMERIC(6,2),
            muscle_signal_strength NUMERIC(6,3),
            actuator_power NUMERIC(6,2),
            device_temperature NUMERIC(5,2),
            usage_duration INT,
            action_type TEXT,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    create_data_mart_table = PostgresOperator(
        task_id='create_data_mart_table',
        postgres_conn_id='airflow_db',
        sql="""
        DROP TABLE IF EXISTS data_mart;
        
        CREATE TABLE data_mart (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            patient_name TEXT NOT NULL,
            device_id TEXT NOT NULL,
            amputation_type TEXT,
            prosthesis_type TEXT,
            order_date DATE,
            delivery_date DATE,
            status TEXT,
            training_sessions INT,
            response_time_target INT,
            -- Агрегированные данные телеметрии
            avg_battery_level NUMERIC(5,2),
            avg_response_time NUMERIC(6,2),
            avg_muscle_signal_strength NUMERIC(6,3),
            avg_actuator_power NUMERIC(6,2),
            avg_device_temperature NUMERIC(5,2),
            total_usage_duration INT,
            action_count INT,
            last_activity_ts TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_data_mart_user_id ON data_mart(user_id);
        CREATE INDEX idx_data_mart_device_id ON data_mart(device_id);
        CREATE INDEX idx_data_mart_status ON data_mart(status);
        """
    )

    extract_crm_task = PythonOperator(
        task_id='extract_crm_task',
        python_callable=extract_crm
    )

    extract_telemetry_task = PythonOperator(
        task_id='extract_telemetry_task',
        python_callable=extract_telemetry
    )

    load_crm_data = PostgresOperator(
        task_id='load_crm_data',
        postgres_conn_id='airflow_db',
        sql="""
        INSERT INTO crm_data (
            id, order_number, total, discount, buyer_id, patient_name, 
            amputation_type, prosthesis_type, order_date, delivery_date, 
            status, training_sessions, response_time_target
        ) VALUES 
        (1, '12345', 150000.00, 15000.00, 'user1', 'User One', 'above_elbow', 'myoelectric', '2024-01-10', '2024-03-15', 'delivered', 8, 95),
        (2, '123456', 120000.00, 5000.00, 'user2', 'User Two', 'below_elbow', 'myoelectric', '2024-01-12', '2024-03-20', 'delivered', 6, 88),
        (3, '123457', 180000.00, 10000.00, 'admin1', 'Admin One', 'above_knee', 'myoelectric', '2024-01-15', '2024-04-01', 'delivered', 10, 105),
        (4, '123458', 80000.00, 0.00, 'prothetic1', 'Prothetic One', 'below_knee', 'mechanical', '2024-01-18', '2024-03-25', 'delivered', 4, 120),
        (5, '123459', 250000.00, 25000.00, 'prothetic2', 'Prothetic Two', 'above_elbow', 'myoelectric', '2024-01-20', '2024-04-10', 'delivered', 12, 85)
        ON CONFLICT (id) DO UPDATE SET
            order_number = EXCLUDED.order_number,
            total = EXCLUDED.total,
            discount = EXCLUDED.discount,
            buyer_id = EXCLUDED.buyer_id,
            patient_name = EXCLUDED.patient_name,
            amputation_type = EXCLUDED.amputation_type,
            prosthesis_type = EXCLUDED.prosthesis_type,
            order_date = EXCLUDED.order_date,
            delivery_date = EXCLUDED.delivery_date,
            status = EXCLUDED.status,
            training_sessions = EXCLUDED.training_sessions,
            response_time_target = EXCLUDED.response_time_target,
            updated_at = CURRENT_TIMESTAMP;
        """
    )

    load_telemetry_data = PostgresOperator(
        task_id='load_telemetry_data',
        postgres_conn_id='airflow_db',
        sql="""
        INSERT INTO telemetry_data (
            device_id, user_id, timestamp, battery_level, response_time, 
            muscle_signal_strength, actuator_power, device_temperature, 
            usage_duration, action_type
        ) VALUES 
        ('PROT001', 'user1', '2024-01-15 08:30:15', 85, 95, 0.75, 120, 32.5, 45, 'grasp'),
        ('PROT001', 'user1', '2024-01-15 08:31:20', 84, 98, 0.72, 115, 32.8, 60, 'release'),
        ('PROT001', 'user1', '2024-01-15 08:32:45', 83, 102, 0.68, 110, 33.1, 75, 'flex'),
        ('PROT001', 'user1', '2024-01-15 08:35:10', 82, 89, 0.71, 125, 32.9, 90, 'extend'),
        ('PROT002', 'user2', '2024-01-15 09:15:30', 92, 88, 0.82, 130, 31.2, 30, 'grasp'),
        ('PROT002', 'user2', '2024-01-15 09:16:45', 91, 91, 0.79, 128, 31.5, 45, 'release'),
        ('PROT002', 'user2', '2024-01-15 09:18:20', 90, 94, 0.76, 125, 31.8, 60, 'flex'),
        ('PROT003', 'admin1', '2024-01-15 10:20:15', 78, 105, 0.65, 115, 34.2, 120, 'grasp'),
        ('PROT003', 'admin1', '2024-01-15 10:22:30', 77, 108, 0.62, 110, 34.5, 135, 'release'),
        ('PROT003', 'admin1', '2024-01-15 10:25:45', 76, 112, 0.58, 105, 34.8, 150, 'flex'),
        ('PROT004', 'prothetic1', '2024-01-15 11:10:20', 88, 92, 0.78, 135, 32.1, 50, 'grasp'),
        ('PROT004', 'prothetic1', '2024-01-15 11:12:35', 87, 95, 0.75, 132, 32.4, 65, 'release'),
        ('PROT005', 'prothetic2', '2024-01-15 14:30:45', 95, 85, 0.88, 140, 30.5, 25, 'grasp'),
        ('PROT005', 'prothetic2', '2024-01-15 14:32:10', 94, 87, 0.85, 138, 30.8, 40, 'release'),
        ('PROT005', 'prothetic2', '2024-01-15 14:35:25', 93, 90, 0.82, 135, 31.1, 55, 'flex'),
        ('PROT001', 'user1', '2024-01-15 16:45:30', 81, 96, 0.69, 120, 33.5, 180, 'grasp'),
        ('PROT001', 'user1', '2024-01-15 16:47:45', 80, 99, 0.66, 118, 33.8, 195, 'release'),
        ('PROT002', 'user2', '2024-01-15 17:20:15', 89, 93, 0.77, 125, 32.2, 90, 'flex'),
        ('PROT002', 'user2', '2024-01-15 17:22:30', 88, 96, 0.74, 122, 32.5, 105, 'extend'),
        ('PROT003', 'admin1', '2024-01-15 18:15:45', 75, 110, 0.60, 108, 35.1, 200, 'grasp'),
        ('PROT003', 'admin1', '2024-01-15 18:18:20', 74, 113, 0.57, 105, 35.4, 215, 'release');
        """
    )

    transform_data_mart_task = PythonOperator(
        task_id='transform_data_mart_task',
        python_callable=transform_data_mart,
        provide_context=True
    )

    load_data_mart_task = PythonOperator(
        task_id='load_data_mart_task',
        python_callable=load_data_mart,
        provide_context=True
    )

    [create_crm_table, create_telemetry_table] >> create_data_mart_table
    create_data_mart_table >> [extract_crm_task, extract_telemetry_task]
    extract_crm_task >> load_crm_data
    extract_telemetry_task >> load_telemetry_data
    [load_crm_data, load_telemetry_data] >> transform_data_mart_task
    transform_data_mart_task >> load_data_mart_task