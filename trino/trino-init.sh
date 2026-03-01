#!/usr/bin/env sh
set -eu

echo "Await trino.."
until trino --server http://trino:8080 --execute "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done

echo "Apply staging DLL..."
trino --server http://trino:8080 --catalog iceberg --schema default \
  --file /initdb/staging/iceberg_staging.sql

echo "Apply detailed DLL..."
trino --server http://trino:8080 --catalog iceberg --schema default \
  --file /initdb/detailed/iceberg_detailed.sql

echo "All done."