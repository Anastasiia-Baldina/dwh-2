-- Auto-generated DETAILED layer DDL for Trino + Iceberg

-- DETAILED layer: user_service_db
CREATE SCHEMA IF NOT EXISTS iceberg.user_service_db WITH (location = 's3://detailed/warehouse/user_service_db');

-- Hubs
CREATE TABLE IF NOT EXISTS iceberg.user_service_db.hub_user (
  hk            varchar,
  bk_user_external_id     uuid,
  load_dts      timestamp(3),
  record_source varchar
);

CREATE TABLE IF NOT EXISTS iceberg.user_service_db.hub_address (
  hk            varchar,
  bk_address_external_id     uuid,
  load_dts      timestamp(3),
  record_source varchar
);

-- Satellites (SCD Type2)
CREATE TABLE IF NOT EXISTS iceberg.user_service_db.sat_user (
  hk_user       varchar,
  hashdiff       varchar,
  effective_from timestamp(3),
  effective_to   timestamp(3),
  is_current     boolean,
  load_dts       timestamp(3),
  record_source  varchar,
  email                          varchar,
  first_name                     varchar,
  last_name                      varchar,
  phone                          varchar,
  date_of_birth                  date,
  registration_date              timestamp(3),
  status                         varchar,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar
);

CREATE TABLE IF NOT EXISTS iceberg.user_service_db.sat_address (
  hk_address       varchar,
  hashdiff       varchar,
  effective_from timestamp(3),
  effective_to   timestamp(3),
  is_current     boolean,
  load_dts       timestamp(3),
  record_source  varchar,
  user_external_id               uuid,
  address_type                   varchar,
  country                        varchar,
  region                         varchar,
  city                           varchar,
  street_address                 varchar,
  postal_code                    varchar,
  apartment                      varchar,
  is_default                     boolean,
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar
);

-- Links
CREATE TABLE IF NOT EXISTS iceberg.user_service_db.lnk_address_user (
  hk_link       varchar,
  hk_address        varchar,
  hk_user       varchar,
  load_dts      timestamp(3),
  record_source varchar
);

-- DETAILED layer: order_service_db
CREATE SCHEMA IF NOT EXISTS iceberg.order_service_db WITH (location = 's3://detailed/warehouse/order_service_db');

-- Hubs
CREATE TABLE IF NOT EXISTS iceberg.order_service_db.hub_order (
  hk            varchar,
  bk_order_external_id     uuid,
  load_dts      timestamp(3),
  record_source varchar
);

CREATE TABLE IF NOT EXISTS iceberg.order_service_db.hub_product (
  hk            varchar,
  bk_product_sku     varchar,
  load_dts      timestamp(3),
  record_source varchar
);

-- Satellites (SCD Type2)
CREATE TABLE IF NOT EXISTS iceberg.order_service_db.sat_order (
  hk_order       varchar,
  hashdiff       varchar,
  effective_from timestamp(3),
  effective_to   timestamp(3),
  is_current     boolean,
  load_dts       timestamp(3),
  record_source  varchar,
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
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar
);

CREATE TABLE IF NOT EXISTS iceberg.order_service_db.sat_product (
  hk_product       varchar,
  hashdiff       varchar,
  effective_from timestamp(3),
  effective_to   timestamp(3),
  is_current     boolean,
  load_dts       timestamp(3),
  record_source  varchar,
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
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar
);

-- Links
CREATE TABLE IF NOT EXISTS iceberg.order_service_db.lnk_order_user (
  hk_link       varchar,
  hk_order        varchar,
  hk_user       varchar,
  load_dts      timestamp(3),
  record_source varchar
);

CREATE TABLE IF NOT EXISTS iceberg.order_service_db.lnk_order_address (
  hk_link       varchar,
  hk_order        varchar,
  hk_address       varchar,
  load_dts      timestamp(3),
  record_source varchar
);

-- DETAILED layer: logistics_service_db
CREATE SCHEMA IF NOT EXISTS iceberg.logistics_service_db WITH (location = 's3://detailed/warehouse/logistics_service_db');

-- Hubs
CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.hub_shipment (
  hk            varchar,
  bk_shipment_external_id     uuid,
  load_dts      timestamp(3),
  record_source varchar
);

CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.hub_warehouse (
  hk            varchar,
  bk_warehouse_code     varchar,
  load_dts      timestamp(3),
  record_source varchar
);

CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.hub_pickup_point (
  hk            varchar,
  bk_pickup_point_code     varchar,
  load_dts      timestamp(3),
  record_source varchar
);

-- Satellites (SCD Type2)
CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.sat_shipment (
  hk_shipment       varchar,
  hashdiff       varchar,
  effective_from timestamp(3),
  effective_to   timestamp(3),
  is_current     boolean,
  load_dts       timestamp(3),
  record_source  varchar,
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
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar
);

CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.sat_warehouse (
  hk_warehouse       varchar,
  hashdiff       varchar,
  effective_from timestamp(3),
  effective_to   timestamp(3),
  is_current     boolean,
  load_dts       timestamp(3),
  record_source  varchar,
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
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar
);

CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.sat_pickup_point (
  hk_pickup_point       varchar,
  hashdiff       varchar,
  effective_from timestamp(3),
  effective_to   timestamp(3),
  is_current     boolean,
  load_dts       timestamp(3),
  record_source  varchar,
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
  created_at                     timestamp(3),
  updated_at                     timestamp(3),
  created_by                     varchar,
  updated_by                     varchar
);

-- Links
CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.lnk_shipment_order (
  hk_link       varchar,
  hk_shipment        varchar,
  hk_order       varchar,
  load_dts      timestamp(3),
  record_source varchar
);

CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.lnk_shipment_warehouse (
  hk_link       varchar,
  hk_shipment        varchar,
  hk_warehouse       varchar,
  load_dts      timestamp(3),
  record_source varchar
);

CREATE TABLE IF NOT EXISTS iceberg.logistics_service_db.lnk_shipment_pickup_point (
  hk_link       varchar,
  hk_shipment        varchar,
  hk_pickup_point       varchar,
  load_dts      timestamp(3),
  record_source varchar
);

