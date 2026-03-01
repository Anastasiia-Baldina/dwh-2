#!/usr/bin/env sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://debezium:8083}"
command -v jq >/dev/null 2>&1 || { echo "ERROR: jq not found"; exit 1; }

upsert() {
  file="$1"
  name="$(jq -r '.name' "$file")"
  cfg="$(jq -c '.config' "$file")"
  echo "Upserting connector: $name"

  # покажем тело ответа при ошибке
  resp="$(mktemp)"
  code="$(curl -sS -o "$resp" -w "%{http_code}" -X PUT \
    -H "Content-Type: application/json" \
    --data "$cfg" \
    "${CONNECT_URL}/connectors/${name}/config" || true)"

  if [ "$code" -lt 200 ] || [ "$code" -ge 300 ]; then
    echo "ERROR: HTTP $code for $name"
    cat "$resp"
    rm -f "$resp"
    exit 1
  fi

  cat "$resp" | jq .
  rm -f "$resp"
}

upsert /debezium/connectors/user_service_db.json
upsert /debezium/connectors/order_service_db.json
upsert /debezium/connectors/logistics_service_db.json

echo "All connectors registered."