DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='debezium') THEN
    CREATE ROLE debezium WITH LOGIN PASSWORD 'debezium' REPLICATION;
  ELSE
    ALTER ROLE debezium WITH LOGIN PASSWORD 'debezium' REPLICATION;
  END IF;
END$$;

-- Даем права на подключение к базам
GRANT CONNECT ON DATABASE user_service_db TO debezium;
GRANT CONNECT ON DATABASE order_service_db TO debezium;
GRANT CONNECT ON DATABASE logistics_service_db TO debezium;

-- Для каждой базы нужно выполнить отдельные команды
-- user_service_db
\c user_service_db
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

DROP PUBLICATION IF EXISTS dbz_user_service;
CREATE PUBLICATION dbz_user_service FOR TABLE users, user_addresses, user_status_history;

-- order_service_db
\c order_service_db
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

DROP PUBLICATION IF EXISTS dbz_order_service;
CREATE PUBLICATION dbz_order_service FOR TABLE orders, products, order_items, order_status_history;

-- logistics_service_db
\c logistics_service_db
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

DROP PUBLICATION IF EXISTS dbz_logistics_service;
CREATE PUBLICATION dbz_logistics_service FOR TABLE warehouses, pickup_points, shipments, shipment_movements, shipment_status_history;