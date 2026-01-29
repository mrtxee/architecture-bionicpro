from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# defaults
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 1),
}


def extract_users(**context):
    hook = PostgresHook(postgres_conn_id="crm_pg")
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        """
        SELECT
            id,
            username,
            email,
            date_of_birth
        FROM users
        """,
        engine,
    )

    context["ti"].xcom_push(key="users", value=df.to_json(orient="records"))


def extract_sensor_data(**context):
    hook = PostgresHook(postgres_conn_id="telemetry_pg")
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        """
        SELECT
            user_id,
            timestamp,
            sensor_value
        FROM sensor_data
        """,
        engine,
    )

    context["ti"].xcom_push(key="sensors", value=df.to_json(orient="records"))


def build_report(**context):
    ti = context["ti"]

    users_df = pd.read_json(
        ti.xcom_pull(task_ids="extract_users", key="users"), orient="records"
    )

    sensors_df = pd.read_json(
        ti.xcom_pull(task_ids="extract_sensor_data", key="sensors"), orient="records"
    )

    users_df["id"] = users_df["id"].astype(str)
    sensors_df["user_id"] = sensors_df["user_id"].astype(str)
    users_df["date_of_birth"] = pd.to_datetime(users_df["date_of_birth"], unit="ms")

    print("Users DF columns:", users_df.columns.tolist())
    print("Sensors DF columns:", sensors_df.columns.tolist())
    print("Users DF head:", users_df.head())
    print("Sensors DF head:", sensors_df.head())
    report_df = users_df.merge(
        sensors_df, left_on="id", right_on="user_id", how="inner"
    )

    report_df = report_df[
        [
            "username",
            "email",
            "date_of_birth",
            "timestamp",
            "sensor_value",
        ]
    ]

    hook = PostgresHook(postgres_conn_id="olap_pg")
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE report")

    report_df.to_sql("report", engine, if_exists="append", index=False)


# DAG setup
with DAG(
    dag_id="dag_crm_telemetry_report",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["simple", "etl"],
) as dag:
    extract_users_task = PythonOperator(
        task_id="extract_users",
        python_callable=extract_users,
        provide_context=True,
    )

    extract_sensor_data_task = PythonOperator(
        task_id="extract_sensor_data",
        python_callable=extract_sensor_data,
        provide_context=True,
    )

    build_report_task = PythonOperator(
        task_id="build_report",
        python_callable=build_report,
        provide_context=True,
    )

    [extract_users_task, extract_sensor_data_task] >> build_report_task
