#!/usr/bin/env sh
set -eu

echo "Await minio api..."
until mc alias set local http://minio:9000 minio minio12345 >/dev/null 2>&1; do
  sleep 2
done

mc mb -p local/staging  || true
mc anonymous set download local/staging  || true

mc mb -p local/detailed || true
mc anonymous set download local/detailed || true

echo "MinIO buckets staging/detailed are ready"