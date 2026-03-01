#!/usr/bin/env sh
set -eu

apk add --no-cache curl jq >/dev/null

# ждём REST API connect
until curl -fsS http://debezium:8083/connectors >/dev/null; do
  sleep 2
done

# регистрируем 3 коннектора
CONNECT_URL=http://debezium:8083 /bin/sh /debezium/register-connectors.sh