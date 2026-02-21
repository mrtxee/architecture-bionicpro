#!/bin/bash

create_connection_if_not_exists() {
  local conn_id=$1
  shift  # Убираем первый аргумент (conn_id), остальное — параметры для `airflow connections add`

  # Проверяем существование подключения
  if airflow connections get "$conn_id" &>/dev/null; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] INFO: Connection '$conn_id' already exists. Skipping creation."
    return 0
  fi

  echo "[$(date +'%Y-%m-%d %H:%M:%S')] INFO: Creating connection '$conn_id'..."

  # Создаём подключение с переданными параметрами
  if airflow connections add "$conn_id" "$@"; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] INFO: Connection '$conn_id' created successfully."
  else
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: Failed to create connection '$conn_id'." >&2
    exit 1
  fi
}

# Создаём подключения только если их нет
create_connection_if_not_exists crm_pg \
  --conn-type postgres \
  --conn-host postgres \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432 \
  --conn-schema crm

create_connection_if_not_exists telemetry_pg \
  --conn-type postgres \
  --conn-host postgres \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432 \
  --conn-schema telemetry

create_connection_if_not_exists olap_pg \
  --conn-type postgres \
  --conn-host postgres \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432 \
  --conn-schema olap