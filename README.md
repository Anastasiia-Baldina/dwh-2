pip install pyyaml
python scripts/generate_source_ddl.py
python scripts/generate_staging_ddl.py
python scripts/generate_detailed_ddl.py

docker-compose down -v
docker-compose up -d

docker exec -it patroni-primary patronictl list


wsl curl http://localhost:8083/connectors

docker run --rm --network hw-2_pgnet curlimages/curl:8.10.1 sh -c "curl -s -o /dev/null -w 'replica /replica => %{http_code}\n' http://patroni-replica:8008/replica"

wsl curl -s http://localhost:8083/connectors/user-service-connector/status
wsl curl -s http://localhost:8083/connectors/order-service-connector/status
wsl curl -s http://localhost:8083/connectors/logistics-service-connector/status

# Статус кластера
docker exec patroni-primary patronictl list

# Конфигурация из DCS
docker exec patroni-primary patronictl show-config

# Содержимое /run/postgres.yml (итоговая конфигурация Patroni)
docker exec patroni-primary cat /run/postgres.yml


1) Список схем 
docker exec -it trino trino --execute "SHOW SCHEMAS FROM iceberg;"
2) Список таблиц
docker exec -it trino trino --execute "SHOW TABLES FROM iceberg.user_service_db;"
docker exec -it trino trino --execute "SHOW TABLES FROM iceberg.order_service_db;"
docker exec -it trino trino --execute "SHOW TABLES FROM iceberg.logistics_service_db;"
3) Структура таблицы 
docker exec -it trino trino --execute "DESCRIBE iceberg.user_service_db.users;"
4) Запрос к данным
docker exec -it trino trino --execute "select * from iceberg.user_service_db.users limit 10;"