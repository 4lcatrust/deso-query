#!/bin/bash
set -e

# Start MinIO in the background
/usr/bin/minio server /data --console-address ":9001" &
MINIO_PID=$!

echo "⏳ Waiting for MinIO to be live on port 9000..."
until curl -s http://localhost:9000/minio/health/live >/dev/null; do
  echo "🔁 Waiting for live check..."
  sleep 2
done

echo "✅ MinIO live. Waiting for mc to connect..."

until mc alias set minio http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; do
  echo "🔁 Retrying mc alias set..."
  sleep 2
done

echo "✅ mc alias set successful. Creating buckets..."
mc mb minio/staging || true
mc mb minio/staging-dq || true
mc mb minio/transformed || true
mc mb minio/transformed-dq || true

echo "✅ Buckets created. Passing control to MinIO process."

wait $MINIO_PID
