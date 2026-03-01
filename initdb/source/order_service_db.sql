\connect order_service_db;
-- database: order_service_db

CREATE TABLE IF NOT EXISTS public.orders (
  order_id           serial PRIMARY KEY,
  order_external_id  uuid UNIQUE,
  user_external_id   uuid,
  order_number       varchar,
  order_date         timestamp,
  status             varchar,
  subtotal           decimal,
  tax_amount         decimal,
  shipping_cost      decimal,
  discount_amount    decimal,
  total_amount       decimal,
  currency           varchar,
  delivery_address_external_id uuid,
  delivery_type      varchar,
  expected_delivery_date date,
  actual_delivery_date date,
  payment_method     varchar,
  payment_status     varchar,
  effective_from     timestamp,
  effective_to       timestamp,
  is_current         boolean,
  created_at         timestamp,
  updated_at         timestamp,
  created_by         varchar,
  updated_by         varchar
);

CREATE TABLE IF NOT EXISTS public.products (
  product_id         serial PRIMARY KEY,
  product_sku        varchar UNIQUE,
  product_name       varchar,
  category           varchar,
  brand              varchar,
  price              decimal,
  currency           varchar,
  weight_grams       integer,
  dimensions_length_cm decimal,
  dimensions_width_cm decimal,
  dimensions_height_cm decimal,
  is_active          boolean,
  effective_from     timestamp,
  effective_to       timestamp,
  is_current         boolean,
  created_at         timestamp,
  updated_at         timestamp,
  created_by         varchar,
  updated_by         varchar
);

CREATE TABLE IF NOT EXISTS public.order_items (
  order_item_id      serial PRIMARY KEY,
  order_external_id  uuid,
  product_sku        varchar,
  quantity           integer,
  unit_price         decimal,
  total_price        decimal,
  product_name_snapshot varchar,
  product_category_snapshot varchar,
  product_brand_snapshot varchar,
  created_at         timestamp,
  updated_at         timestamp,
  created_by         varchar,
  updated_by         varchar,
  CONSTRAINT fk_order_items__order_external_id FOREIGN KEY (order_external_id) REFERENCES public.orders(order_external_id),
  CONSTRAINT fk_order_items__product_sku FOREIGN KEY (product_sku) REFERENCES public.products(product_sku)
);

CREATE TABLE IF NOT EXISTS public.order_status_history (
  history_id         serial PRIMARY KEY,
  order_external_id  uuid,
  old_status         varchar,
  new_status         varchar,
  change_reason      varchar,
  changed_at         timestamp,
  changed_by         varchar,
  session_id         varchar,
  ip_address         inet,
  notes              text,
  CONSTRAINT fk_order_status_history__order_external_id FOREIGN KEY (order_external_id) REFERENCES public.orders(order_external_id)
);

