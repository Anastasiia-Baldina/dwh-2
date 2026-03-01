-- Auto-generated STAGING layer DDL 

-- STAGING layer: user_service_db
CREATE SCHEMA IF NOT EXISTS iceberg.user_service_db WITH (location = 's3://staging/warehouse/user_service_db');

-- from source table: user_service_db.users
CREATE TABLE IF NOT EXISTS iceberg.user_service_db.users (
  user_id                        bigint,
  user_external_id               uuid,
  email                          varchar,
  first_name                     varchar,
  last_name                      varchar,
  phone                          varchar,
  date_of_birth                  date,
  registration_date              timestamp(3),
  status                         varchar,
  effective_from                 timestamp(3),
  effective_to                   timestamp(3),
  is_current                     boolean,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: user_service_db.user_addresses
CREATE TABLE IF NOT EXISTS iceberg.user_service_db.user_addresses (
  address_id                     bigint,
  address_external_id            uuid,
  user_external_id               uuid,
  address_type                   varchar,
  country                        varchar,
  region                         varchar,
  city                           varchar,
  street_address                 varchar,
  postal_code                    varchar,
  apartment                      varchar,
  is_default                     boolean,
  effective_from                 timestamp(3),
  effective_to                   timestamp(3),
  is_current                     boolean,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: user_service_db.user_status_history
CREATE TABLE IF NOT EXISTS iceberg.user_service_db.user_status_history (
  history_id                     bigint,
  user_external_id               uuid,
  old_status                     varchar,
  new_status                     varchar,
  change_reason                  varchar,
  changed_at                     timestamp(3),
  changed_by                     varchar,
  session_id                     varchar,
  ip_address                     varchar(64),
  user_agent                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- STAGING layer: order_service_db
CREATE SCHEMA IF NOT EXISTS iceberg.order_service_db WITH (location = 's3://staging/warehouse/order_service_db');

-- from source table: order_service_db.orders
CREATE TABLE IF NOT EXISTS iceberg.order_service_db.orders (
  order_id                       bigint,
  order_external_id              uuid,
  user_external_id               uuid,
  order_number                   varchar,
  order_date                     timestamp(3),
  status                         varchar,
  subtotal                       decimal(38,10),
  tax_amount                     decimal(38,10),
  shipping_cost                  decimal(38,10),
  discount_amount                decimal(38,10),
  total_amount                   decimal(38,10),
  currency                       varchar,
  delivery_address_external_id   uuid,
  delivery_type                  varchar,
  expected_delivery_date         date,
  actual_delivery_date           date,
  payment_method                 varchar,
  payment_status                 varchar,
  effective_from                 timestamp(3),
  effective_to                   timestamp(3),
  is_current                     boolean,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: order_service_db.products
CREATE TABLE IF NOT EXISTS iceberg.order_service_db.products (
  product_id                     bigint,
  product_sku                    varchar,
  product_name                   varchar,
  category                       varchar,
  brand                          varchar,
  price                          decimal(38,10),
  currency                       varchar,
  weight_grams                   integer,
  dimensions_length_cm           decimal(38,10),
  dimensions_width_cm            decimal(38,10),
  dimensions_height_cm           decimal(38,10),
  is_active                      boolean,
  effective_from                 timestamp(3),
  effective_to                   timestamp(3),
  is_current                     boolean,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: order_service_db.order_items
CREATE TABLE IF NOT EXISTS iceberg.order_service_db.order_items (
  order_item_id                  bigint,
  order_external_id              uuid,
  product_sku                    varchar,
  quantity                       integer,
  unit_price                     decimal(38,10),
  total_price                    decimal(38,10),
  product_name_snapshot          varchar,
  product_category_snapshot      varchar,
  product_brand_snapshot         varchar,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: order_service_db.order_status_history
CREATE TABLE IF NOT EXISTS iceberg.order_service_db.order_status_history (
  history_id                     bigint,
  order_external_id              uuid,
  old_status                     varchar,
  new_status                     varchar,
  change_reason                  varchar,
  changed_at                     timestamp(3),
  changed_by                     varchar,
  session_id                     varchar,
  ip_address                     varchar(64),
  notes                          varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- STAGING layer: logistics_service_db
CREATE SCHEMA IF NOT EXISTS iceberg.logistics_service_db WITH (location = 's3://staging/warehouse/logistics_service_db');

-- from source table: logistics_service_db.warehouses
CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.warehouses (
  warehouse_id                   bigint,
  warehouse_code                 varchar,
  warehouse_name                 varchar,
  warehouse_type                 varchar,
  country                        varchar,
  region                         varchar,
  city                           varchar,
  street_address                 varchar,
  postal_code                    varchar,
  is_active                      boolean,
  max_capacity_cubic_meters      decimal(38,10),
  operating_hours                varchar,
  contact_phone                  varchar,
  manager_name                   varchar,
  effective_from                 timestamp(3),
  effective_to                   timestamp(3),
  is_current                     boolean,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: logistics_service_db.pickup_points
CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.pickup_points (
  pickup_point_id                bigint,
  pickup_point_code              varchar,
  pickup_point_name              varchar,
  pickup_point_type              varchar,
  country                        varchar,
  region                         varchar,
  city                           varchar,
  street_address                 varchar,
  postal_code                    varchar,
  is_active                      boolean,
  max_capacity_packages          integer,
  operating_hours                varchar,
  contact_phone                  varchar,
  partner_name                   varchar,
  effective_from                 timestamp(3),
  effective_to                   timestamp(3),
  is_current                     boolean,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: logistics_service_db.shipments
CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.shipments (
  shipment_id                    bigint,
  shipment_external_id           uuid,
  order_external_id              uuid,
  tracking_number                varchar,
  status                         varchar,
  weight_grams                   integer,
  volume_cubic_cm                integer,
  package_count                  integer,
  origin_warehouse_code          varchar,
  destination_type               varchar,
  destination_pickup_point_code  varchar,
  destination_address_external_id uuid,
  created_date                   timestamp(3),
  dispatched_date                timestamp(3),
  estimated_delivery_date        timestamp(3),
  actual_delivery_date           timestamp(3),
  delivery_notes                 varchar,
  recipient_name                 varchar,
  delivery_signature             varchar,
  effective_from                 timestamp(3),
  effective_to                   timestamp(3),
  is_current                     boolean,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: logistics_service_db.shipment_movements
CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.shipment_movements (
  movement_id                    bigint,
  shipment_external_id           uuid,
  movement_type                  varchar,
  location_type                  varchar,
  location_code                  varchar,
  movement_datetime              timestamp(3),
  operator_name                  varchar,
  notes                          varchar,
  latitude                       decimal(38,10),
  longitude                      decimal(38,10),
  created_at                     timestamp(3),
  created_by                     varchar,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

-- from source table: logistics_service_db.shipment_status_history
CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.shipment_status_history (
  history_id                     bigint,
  shipment_external_id           uuid,
  old_status                     varchar,
  new_status                     varchar,
  change_reason                  varchar,
  changed_at                     timestamp(3),
  changed_by                     varchar,
  location_type                  varchar,
  location_code                  varchar,
  notes                          varchar,
  customer_notified              boolean,
  __op                           varchar,
  __ts_ms                        bigint,
  __kafka_topic                  varchar,
  __kafka_partition              integer,
  __kafka_offset                 bigint,
  __ingest_dts                   timestamp(3),
  __record_source                varchar
);

