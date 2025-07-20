#!/bin/bash
set -e

echo "🔄 Initializing Airflow database..."
airflow db init
airflow db upgrade

# Ensure Airflow CLI is available
export PATH="$HOME/.local/bin:$PATH"

# Function to safely add connections
add_connection_if_not_exists() {
    local conn_id=$1
    shift
    if ! airflow connections list 2>/dev/null | grep -q "^$conn_id "; then
        echo "🔗 Creating connection: $conn_id"
        airflow connections add "$conn_id" "$@" || echo "⚠️ Failed to create connection $conn_id"
    else
        echo "🔗 Connection '$conn_id' already exists, skipping"
    fi
}

# Check if user already exists to avoid errors on restart
if ! airflow users list 2>/dev/null | grep -q "admin"; then
    echo "👤 Creating admin user..."
    airflow users create \
      --username admin \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@example.com \
      --password admin
else
    echo "👤 Admin user already exists, skipping creation"
fi

echo "⏳ Waiting for services to be ready..."

# Wait for PostgreSQL
echo "🔍 Checking PostgreSQL..."
until pg_isready -h postgres_db -p 5432 -U spark 2>/dev/null; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

# Wait for ClickHouse
echo "🔍 Checking ClickHouse..."
until curl -s http://clickhouse_db:8123/ping > /dev/null 2>&1; do
    echo "Waiting for ClickHouse..."
    sleep 2
done

# Wait for MinIO
echo "🔍 Checking MinIO..."
until curl -s http://minio:9000/minio/health/live > /dev/null 2>&1; do
    echo "Waiting for MinIO..."
    sleep 2
done

# Wait for Spark Master
echo "🔍 Checking Spark Master..."
until curl -s http://spark-master:8080 > /dev/null 2>&1; do
    echo "Waiting for Spark Master..."
    sleep 2
done

echo "🔗 Creating connections..."

# Delete existing connections (ignore errors if not present)
airflow connections delete spark || true
airflow connections delete postgres || true
airflow connections delete clickhouse || true
airflow connections delete minio || true

# Spark connection
add_connection_if_not_exists 'spark' \
  --conn-type 'spark' \
  --conn-host 'spark://spark-master' \
  --conn-port 7077 \
  --conn-extra '{"deploy_mode": "cluster", "spark_binary": "spark-submit"}'

# PostgreSQL connection
add_connection_if_not_exists 'postgres' \
  --conn-type 'postgres' \
  --conn-host 'postgres_db' \
  --conn-port 5432 \
  --conn-login 'postgres' \
  --conn-password 'postgres' \
  --conn-schema 'public'

# ClickHouse connection
add_connection_if_not_exists 'clickhouse' \
  --conn-type 'http' \
  --conn-host 'clickhouse_db' \
  --conn-port 8123 \
  --conn-login 'clickhouse' \
  --conn-password 'clickhouse' \

# MinIO connection
add_connection_if_not_exists 'minio' \
  --conn-type 'aws' \
  --conn-host 'http://minio:9000' \
  --conn-login 'minioadmin' \
  --conn-password 'minioadmin' \
  --conn-extra '{"endpoint_url": "http://minio:9000"}'

echo "📊 Importing variables..."
if [[ -f /opt/airflow/variables.json ]]; then
    if airflow variables import /opt/airflow/variables.json; then
        echo "✅ Variables imported successfully"
    else
        echo "⚠️ Failed to import variables, but continuing..."
    fi
else
    echo "📊 No variables.json file found, skipping variable import"
fi

echo "🎉 Airflow initialization completed successfully!"
echo "🌐 Web UI will be available at http://localhost:8080 (admin/admin)"