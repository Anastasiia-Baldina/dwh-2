INSERT INTO iceberg.user_service_db.hub_user (hk, bk_user_external_id, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(cast(u.user_external_id AS varchar))))) AS hk,
  u.user_external_id AS bk_user_external_id,
  coalesce(u.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(u.__record_source, 'user_service_db.users') AS record_source
FROM iceberg.user_service_db.users u
WHERE u.is_current = true
  AND u.user_external_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.user_service_db.hub_user h
    WHERE h.bk_user_external_id = u.user_external_id
  );

INSERT INTO iceberg.user_service_db.hub_address (hk, bk_address_external_id, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(cast(a.address_external_id AS varchar))))) AS hk,
  a.address_external_id AS bk_address_external_id,
  coalesce(a.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(a.__record_source, 'user_service_db.user_addresses') AS record_source
FROM iceberg.user_service_db.user_addresses a
WHERE a.is_current = true
  AND a.address_external_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.user_service_db.hub_address h
    WHERE h.bk_address_external_id = a.address_external_id
  );

UPDATE iceberg.user_service_db.sat_user
SET effective_to = cast(current_timestamp AS timestamp(3)),
    is_current = false
WHERE is_current = true
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT
        hu.hk AS src_hk_user,
        lower(to_hex(md5(to_utf8(concat(
          coalesce(cast(u.email AS varchar), ''), '|',
          coalesce(cast(u.first_name AS varchar), ''), '|',
          coalesce(cast(u.last_name AS varchar), ''), '|',
          coalesce(cast(u.phone AS varchar), ''), '|',
          coalesce(cast(u.date_of_birth AS varchar), ''), '|',
          coalesce(cast(u.registration_date AS varchar), ''), '|',
          coalesce(cast(u.status AS varchar), ''), '|',
          coalesce(cast(u.created_at AS varchar), ''), '|',
          coalesce(cast(u.updated_at AS varchar), ''), '|',
          coalesce(cast(u.created_by AS varchar), ''), '|',
          coalesce(cast(u.updated_by AS varchar), '')
        ))))) AS src_hashdiff
      FROM iceberg.user_service_db.users u
      JOIN iceberg.user_service_db.hub_user hu
        ON hu.bk_user_external_id = u.user_external_id
      WHERE u.is_current = true
        AND u.user_external_id IS NOT NULL
    ) src
    WHERE src.src_hk_user = hk_user
      AND src.src_hashdiff <> hashdiff
  );

INSERT INTO iceberg.user_service_db.sat_user (
  hk_user, hashdiff, effective_from, effective_to, is_current, load_dts, record_source,
  email, first_name, last_name, phone, date_of_birth, registration_date, status,
  created_at, updated_at, created_by, updated_by
)
SELECT
  src.hk_user,
  src.hashdiff,
  cast(current_timestamp AS timestamp(3)),
  TIMESTAMP '9999-12-31 00:00:00.000',
  true,
  src.load_dts,
  src.record_source,
  src.email,
  src.first_name,
  src.last_name,
  src.phone,
  src.date_of_birth,
  src.registration_date,
  src.status,
  src.created_at,
  src.updated_at,
  src.created_by,
  src.updated_by
FROM (
  SELECT
    hu.hk AS hk_user,
    lower(to_hex(md5(to_utf8(concat(
      coalesce(cast(u.email AS varchar), ''), '|',
      coalesce(cast(u.first_name AS varchar), ''), '|',
      coalesce(cast(u.last_name AS varchar), ''), '|',
      coalesce(cast(u.phone AS varchar), ''), '|',
      coalesce(cast(u.date_of_birth AS varchar), ''), '|',
      coalesce(cast(u.registration_date AS varchar), ''), '|',
      coalesce(cast(u.status AS varchar), ''), '|',
      coalesce(cast(u.created_at AS varchar), ''), '|',
      coalesce(cast(u.updated_at AS varchar), ''), '|',
      coalesce(cast(u.created_by AS varchar), ''), '|',
      coalesce(cast(u.updated_by AS varchar), '')
    ))))) AS hashdiff,
    coalesce(u.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
    coalesce(u.__record_source, 'user_service_db.users') AS record_source,
    u.email,
    u.first_name,
    u.last_name,
    u.phone,
    u.date_of_birth,
    u.registration_date,
    u.status,
    u.created_at,
    u.updated_at,
    u.created_by,
    u.updated_by
  FROM iceberg.user_service_db.users u
  JOIN iceberg.user_service_db.hub_user hu
    ON hu.bk_user_external_id = u.user_external_id
  WHERE u.is_current = true
    AND u.user_external_id IS NOT NULL
) src
LEFT JOIN iceberg.user_service_db.sat_user cur
  ON cur.hk_user = src.hk_user AND cur.is_current = true
WHERE cur.hk_user IS NULL OR cur.hashdiff <> src.hashdiff;

UPDATE iceberg.user_service_db.sat_address
SET effective_to = cast(current_timestamp AS timestamp(3)),
    is_current = false
WHERE is_current = true
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT
        ha.hk AS src_hk_address,
        lower(to_hex(md5(to_utf8(concat(
          coalesce(cast(a.user_external_id AS varchar), ''), '|',
          coalesce(cast(a.address_type AS varchar), ''), '|',
          coalesce(cast(a.country AS varchar), ''), '|',
          coalesce(cast(a.region AS varchar), ''), '|',
          coalesce(cast(a.city AS varchar), ''), '|',
          coalesce(cast(a.street_address AS varchar), ''), '|',
          coalesce(cast(a.postal_code AS varchar), ''), '|',
          coalesce(cast(a.apartment AS varchar), ''), '|',
          coalesce(cast(a.is_default AS varchar), ''), '|',
          coalesce(cast(a.created_at AS varchar), ''), '|',
          coalesce(cast(a.updated_at AS varchar), ''), '|',
          coalesce(cast(a.created_by AS varchar), ''), '|',
          coalesce(cast(a.updated_by AS varchar), '')
        ))))) AS src_hashdiff
      FROM iceberg.user_service_db.user_addresses a
      JOIN iceberg.user_service_db.hub_address ha
        ON ha.bk_address_external_id = a.address_external_id
      WHERE a.is_current = true
        AND a.address_external_id IS NOT NULL
    ) src
    WHERE src.src_hk_address = hk_address
      AND src.src_hashdiff <> hashdiff
  );

INSERT INTO iceberg.user_service_db.sat_address (
  hk_address, hashdiff, effective_from, effective_to, is_current, load_dts, record_source,
  user_external_id, address_type, country, region, city, street_address, postal_code, apartment, is_default,
  created_at, updated_at, created_by, updated_by
)
SELECT
  src.hk_address,
  src.hashdiff,
  cast(current_timestamp AS timestamp(3)),
  TIMESTAMP '9999-12-31 00:00:00.000',
  true,
  src.load_dts,
  src.record_source,
  src.user_external_id,
  src.address_type,
  src.country,
  src.region,
  src.city,
  src.street_address,
  src.postal_code,
  src.apartment,
  src.is_default,
  src.created_at,
  src.updated_at,
  src.created_by,
  src.updated_by
FROM (
  SELECT
    ha.hk AS hk_address,
    lower(to_hex(md5(to_utf8(concat(
      coalesce(cast(a.user_external_id AS varchar), ''), '|',
      coalesce(cast(a.address_type AS varchar), ''), '|',
      coalesce(cast(a.country AS varchar), ''), '|',
      coalesce(cast(a.region AS varchar), ''), '|',
      coalesce(cast(a.city AS varchar), ''), '|',
      coalesce(cast(a.street_address AS varchar), ''), '|',
      coalesce(cast(a.postal_code AS varchar), ''), '|',
      coalesce(cast(a.apartment AS varchar), ''), '|',
      coalesce(cast(a.is_default AS varchar), ''), '|',
      coalesce(cast(a.created_at AS varchar), ''), '|',
      coalesce(cast(a.updated_at AS varchar), ''), '|',
      coalesce(cast(a.created_by AS varchar), ''), '|',
      coalesce(cast(a.updated_by AS varchar), '')
    ))))) AS hashdiff,
    coalesce(a.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
    coalesce(a.__record_source, 'user_service_db.user_addresses') AS record_source,
    a.user_external_id,
    a.address_type,
    a.country,
    a.region,
    a.city,
    a.street_address,
    a.postal_code,
    a.apartment,
    a.is_default,
    a.created_at,
    a.updated_at,
    a.created_by,
    a.updated_by
  FROM iceberg.user_service_db.user_addresses a
  JOIN iceberg.user_service_db.hub_address ha
    ON ha.bk_address_external_id = a.address_external_id
  WHERE a.is_current = true
    AND a.address_external_id IS NOT NULL
) src
LEFT JOIN iceberg.user_service_db.sat_address cur
  ON cur.hk_address = src.hk_address AND cur.is_current = true
WHERE cur.hk_address IS NULL OR cur.hashdiff <> src.hashdiff;

INSERT INTO iceberg.user_service_db.lnk_address_user (hk_link, hk_address, hk_user, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(concat(ha.hk, '|', hu.hk))))) AS hk_link,
  ha.hk AS hk_address,
  hu.hk AS hk_user,
  coalesce(a.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(a.__record_source, 'user_service_db.user_addresses') AS record_source
FROM iceberg.user_service_db.user_addresses a
JOIN iceberg.user_service_db.hub_address ha
  ON ha.bk_address_external_id = a.address_external_id
JOIN iceberg.user_service_db.hub_user hu
  ON hu.bk_user_external_id = a.user_external_id
WHERE a.is_current = true
  AND a.address_external_id IS NOT NULL
  AND a.user_external_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.user_service_db.lnk_address_user l
    WHERE l.hk_address = ha.hk AND l.hk_user = hu.hk
  );

INSERT INTO iceberg.order_service_db.hub_order (hk, bk_order_external_id, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(cast(o.order_external_id AS varchar))))) AS hk,
  o.order_external_id AS bk_order_external_id,
  coalesce(o.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(o.__record_source, 'order_service_db.orders') AS record_source
FROM iceberg.order_service_db.orders o
WHERE o.is_current = true
  AND o.order_external_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.order_service_db.hub_order h
    WHERE h.bk_order_external_id = o.order_external_id
  );

INSERT INTO iceberg.order_service_db.hub_product (hk, bk_product_sku, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(cast(p.product_sku AS varchar))))) AS hk,
  p.product_sku AS bk_product_sku,
  coalesce(p.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(p.__record_source, 'order_service_db.products') AS record_source
FROM iceberg.order_service_db.products p
WHERE p.is_current = true
  AND p.product_sku IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.order_service_db.hub_product h
    WHERE h.bk_product_sku = p.product_sku
  );

UPDATE iceberg.order_service_db.sat_order
SET effective_to = cast(current_timestamp AS timestamp(3)),
    is_current = false
WHERE is_current = true
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT
        ho.hk AS src_hk_order,
        lower(to_hex(md5(to_utf8(concat(
          coalesce(cast(o.user_external_id AS varchar), ''), '|',
          coalesce(cast(o.order_number AS varchar), ''), '|',
          coalesce(cast(o.order_date AS varchar), ''), '|',
          coalesce(cast(o.status AS varchar), ''), '|',
          coalesce(cast(o.subtotal AS varchar), ''), '|',
          coalesce(cast(o.tax_amount AS varchar), ''), '|',
          coalesce(cast(o.shipping_cost AS varchar), ''), '|',
          coalesce(cast(o.discount_amount AS varchar), ''), '|',
          coalesce(cast(o.total_amount AS varchar), ''), '|',
          coalesce(cast(o.currency AS varchar), ''), '|',
          coalesce(cast(o.delivery_address_external_id AS varchar), ''), '|',
          coalesce(cast(o.delivery_type AS varchar), ''), '|',
          coalesce(cast(o.expected_delivery_date AS varchar), ''), '|',
          coalesce(cast(o.actual_delivery_date AS varchar), ''), '|',
          coalesce(cast(o.payment_method AS varchar), ''), '|',
          coalesce(cast(o.payment_status AS varchar), ''), '|',
          coalesce(cast(o.created_at AS varchar), ''), '|',
          coalesce(cast(o.updated_at AS varchar), ''), '|',
          coalesce(cast(o.created_by AS varchar), ''), '|',
          coalesce(cast(o.updated_by AS varchar), '')
        ))))) AS src_hashdiff
      FROM iceberg.order_service_db.orders o
      JOIN iceberg.order_service_db.hub_order ho
        ON ho.bk_order_external_id = o.order_external_id
      WHERE o.is_current = true
        AND o.order_external_id IS NOT NULL
    ) src
    WHERE src.src_hk_order = hk_order
      AND src.src_hashdiff <> hashdiff
  );

INSERT INTO iceberg.order_service_db.sat_order (
  hk_order, hashdiff, effective_from, effective_to, is_current, load_dts, record_source,
  user_external_id, order_number, order_date, status,
  subtotal, tax_amount, shipping_cost, discount_amount, total_amount, currency,
  delivery_address_external_id, delivery_type, expected_delivery_date, actual_delivery_date,
  payment_method, payment_status, created_at, updated_at, created_by, updated_by
)
SELECT
  src.hk_order,
  src.hashdiff,
  cast(current_timestamp AS timestamp(3)),
  TIMESTAMP '9999-12-31 00:00:00.000',
  true,
  src.load_dts,
  src.record_source,
  src.user_external_id,
  src.order_number,
  src.order_date,
  src.status,
  src.subtotal,
  src.tax_amount,
  src.shipping_cost,
  src.discount_amount,
  src.total_amount,
  src.currency,
  src.delivery_address_external_id,
  src.delivery_type,
  src.expected_delivery_date,
  src.actual_delivery_date,
  src.payment_method,
  src.payment_status,
  src.created_at,
  src.updated_at,
  src.created_by,
  src.updated_by
FROM (
  SELECT
    ho.hk AS hk_order,
    lower(to_hex(md5(to_utf8(concat(
      coalesce(cast(o.user_external_id AS varchar), ''), '|',
      coalesce(cast(o.order_number AS varchar), ''), '|',
      coalesce(cast(o.order_date AS varchar), ''), '|',
      coalesce(cast(o.status AS varchar), ''), '|',
      coalesce(cast(o.subtotal AS varchar), ''), '|',
      coalesce(cast(o.tax_amount AS varchar), ''), '|',
      coalesce(cast(o.shipping_cost AS varchar), ''), '|',
      coalesce(cast(o.discount_amount AS varchar), ''), '|',
      coalesce(cast(o.total_amount AS varchar), ''), '|',
      coalesce(cast(o.currency AS varchar), ''), '|',
      coalesce(cast(o.delivery_address_external_id AS varchar), ''), '|',
      coalesce(cast(o.delivery_type AS varchar), ''), '|',
      coalesce(cast(o.expected_delivery_date AS varchar), ''), '|',
      coalesce(cast(o.actual_delivery_date AS varchar), ''), '|',
      coalesce(cast(o.payment_method AS varchar), ''), '|',
      coalesce(cast(o.payment_status AS varchar), ''), '|',
      coalesce(cast(o.created_at AS varchar), ''), '|',
      coalesce(cast(o.updated_at AS varchar), ''), '|',
      coalesce(cast(o.created_by AS varchar), ''), '|',
      coalesce(cast(o.updated_by AS varchar), '')
    ))))) AS hashdiff,
    coalesce(o.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
    coalesce(o.__record_source, 'order_service_db.orders') AS record_source,
    o.user_external_id,
    o.order_number,
    o.order_date,
    o.status,
    o.subtotal,
    o.tax_amount,
    o.shipping_cost,
    o.discount_amount,
    o.total_amount,
    o.currency,
    o.delivery_address_external_id,
    o.delivery_type,
    o.expected_delivery_date,
    o.actual_delivery_date,
    o.payment_method,
    o.payment_status,
    o.created_at,
    o.updated_at,
    o.created_by,
    o.updated_by
  FROM iceberg.order_service_db.orders o
  JOIN iceberg.order_service_db.hub_order ho
    ON ho.bk_order_external_id = o.order_external_id
  WHERE o.is_current = true
    AND o.order_external_id IS NOT NULL
) src
LEFT JOIN iceberg.order_service_db.sat_order cur
  ON cur.hk_order = src.hk_order AND cur.is_current = true
WHERE cur.hk_order IS NULL OR cur.hashdiff <> src.hashdiff;

UPDATE iceberg.order_service_db.sat_product
SET effective_to = cast(current_timestamp AS timestamp(3)),
    is_current = false
WHERE is_current = true
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT
        hp.hk AS src_hk_product,
        lower(to_hex(md5(to_utf8(concat(
          coalesce(cast(p.product_name AS varchar), ''), '|',
          coalesce(cast(p.category AS varchar), ''), '|',
          coalesce(cast(p.brand AS varchar), ''), '|',
          coalesce(cast(p.price AS varchar), ''), '|',
          coalesce(cast(p.currency AS varchar), ''), '|',
          coalesce(cast(p.weight_grams AS varchar), ''), '|',
          coalesce(cast(p.dimensions_length_cm AS varchar), ''), '|',
          coalesce(cast(p.dimensions_width_cm AS varchar), ''), '|',
          coalesce(cast(p.dimensions_height_cm AS varchar), ''), '|',
          coalesce(cast(p.is_active AS varchar), ''), '|',
          coalesce(cast(p.created_at AS varchar), ''), '|',
          coalesce(cast(p.updated_at AS varchar), ''), '|',
          coalesce(cast(p.created_by AS varchar), ''), '|',
          coalesce(cast(p.updated_by AS varchar), '')
        ))))) AS src_hashdiff
      FROM iceberg.order_service_db.products p
      JOIN iceberg.order_service_db.hub_product hp
        ON hp.bk_product_sku = p.product_sku
      WHERE p.is_current = true
        AND p.product_sku IS NOT NULL
    ) src
    WHERE src.src_hk_product = hk_product
      AND src.src_hashdiff <> hashdiff
  );

INSERT INTO iceberg.order_service_db.sat_product (
  hk_product, hashdiff, effective_from, effective_to, is_current, load_dts, record_source,
  product_name, category, brand, price, currency, weight_grams,
  dimensions_length_cm, dimensions_width_cm, dimensions_height_cm,
  is_active, created_at, updated_at, created_by, updated_by
)
SELECT
  src.hk_product,
  src.hashdiff,
  cast(current_timestamp AS timestamp(3)),
  TIMESTAMP '9999-12-31 00:00:00.000',
  true,
  src.load_dts,
  src.record_source,
  src.product_name,
  src.category,
  src.brand,
  src.price,
  src.currency,
  src.weight_grams,
  src.dimensions_length_cm,
  src.dimensions_width_cm,
  src.dimensions_height_cm,
  src.is_active,
  src.created_at,
  src.updated_at,
  src.created_by,
  src.updated_by
FROM (
  SELECT
    hp.hk AS hk_product,
    lower(to_hex(md5(to_utf8(concat(
      coalesce(cast(p.product_name AS varchar), ''), '|',
      coalesce(cast(p.category AS varchar), ''), '|',
      coalesce(cast(p.brand AS varchar), ''), '|',
      coalesce(cast(p.price AS varchar), ''), '|',
      coalesce(cast(p.currency AS varchar), ''), '|',
      coalesce(cast(p.weight_grams AS varchar), ''), '|',
      coalesce(cast(p.dimensions_length_cm AS varchar), ''), '|',
      coalesce(cast(p.dimensions_width_cm AS varchar), ''), '|',
      coalesce(cast(p.dimensions_height_cm AS varchar), ''), '|',
      coalesce(cast(p.is_active AS varchar), ''), '|',
      coalesce(cast(p.created_at AS varchar), ''), '|',
      coalesce(cast(p.updated_at AS varchar), ''), '|',
      coalesce(cast(p.created_by AS varchar), ''), '|',
      coalesce(cast(p.updated_by AS varchar), '')
    ))))) AS hashdiff,
    coalesce(p.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
    coalesce(p.__record_source, 'order_service_db.products') AS record_source,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.currency,
    p.weight_grams,
    p.dimensions_length_cm,
    p.dimensions_width_cm,
    p.dimensions_height_cm,
    p.is_active,
    p.created_at,
    p.updated_at,
    p.created_by,
    p.updated_by
  FROM iceberg.order_service_db.products p
  JOIN iceberg.order_service_db.hub_product hp
    ON hp.bk_product_sku = p.product_sku
  WHERE p.is_current = true
    AND p.product_sku IS NOT NULL
) src
LEFT JOIN iceberg.order_service_db.sat_product cur
  ON cur.hk_product = src.hk_product AND cur.is_current = true
WHERE cur.hk_product IS NULL OR cur.hashdiff <> src.hashdiff;

INSERT INTO iceberg.order_service_db.lnk_order_user (hk_link, hk_order, hk_user, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(concat(ho.hk, '|', hu.hk))))) AS hk_link,
  ho.hk AS hk_order,
  hu.hk AS hk_user,
  coalesce(o.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(o.__record_source, 'order_service_db.orders') AS record_source
FROM iceberg.order_service_db.orders o
JOIN iceberg.order_service_db.hub_order ho
  ON ho.bk_order_external_id = o.order_external_id
JOIN iceberg.user_service_db.hub_user hu
  ON hu.bk_user_external_id = o.user_external_id
WHERE o.is_current = true
  AND o.order_external_id IS NOT NULL
  AND o.user_external_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.order_service_db.lnk_order_user l
    WHERE l.hk_order = ho.hk AND l.hk_user = hu.hk
  );

INSERT INTO iceberg.order_service_db.lnk_order_address (hk_link, hk_order, hk_address, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(concat(ho.hk, '|', ha.hk))))) AS hk_link,
  ho.hk AS hk_order,
  ha.hk AS hk_address,
  coalesce(o.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(o.__record_source, 'order_service_db.orders') AS record_source
FROM iceberg.order_service_db.orders o
JOIN iceberg.order_service_db.hub_order ho
  ON ho.bk_order_external_id = o.order_external_id
JOIN iceberg.user_service_db.hub_address ha
  ON ha.bk_address_external_id = o.delivery_address_external_id
WHERE o.is_current = true
  AND o.order_external_id IS NOT NULL
  AND o.delivery_address_external_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.order_service_db.lnk_order_address l
    WHERE l.hk_order = ho.hk AND l.hk_address = ha.hk
  );

INSERT INTO iceberg.logistics_service_db.hub_shipment (hk, bk_shipment_external_id, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(cast(s.shipment_external_id AS varchar))))) AS hk,
  s.shipment_external_id AS bk_shipment_external_id,
  coalesce(s.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(s.__record_source, 'logistics_service_db.shipments') AS record_source
FROM iceberg.logistics_service_db.shipments s
WHERE s.is_current = true
  AND s.shipment_external_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.logistics_service_db.hub_shipment h
    WHERE h.bk_shipment_external_id = s.shipment_external_id
  );

INSERT INTO iceberg.logistics_service_db.hub_warehouse (hk, bk_warehouse_code, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(cast(w.warehouse_code AS varchar))))) AS hk,
  w.warehouse_code AS bk_warehouse_code,
  coalesce(w.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(w.__record_source, 'logistics_service_db.warehouses') AS record_source
FROM iceberg.logistics_service_db.warehouses w
WHERE w.is_current = true
  AND w.warehouse_code IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.logistics_service_db.hub_warehouse h
    WHERE h.bk_warehouse_code = w.warehouse_code
  );

INSERT INTO iceberg.logistics_service_db.hub_pickup_point (hk, bk_pickup_point_code, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(cast(p.pickup_point_code AS varchar))))) AS hk,
  p.pickup_point_code AS bk_pickup_point_code,
  coalesce(p.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(p.__record_source, 'logistics_service_db.pickup_points') AS record_source
FROM iceberg.logistics_service_db.pickup_points p
WHERE p.is_current = true
  AND p.pickup_point_code IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.logistics_service_db.hub_pickup_point h
    WHERE h.bk_pickup_point_code = p.pickup_point_code
  );

UPDATE iceberg.logistics_service_db.sat_shipment
SET effective_to = cast(current_timestamp AS timestamp(3)),
    is_current = false
WHERE is_current = true
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT
        hs.hk AS src_hk_shipment,
        lower(to_hex(md5(to_utf8(concat(
          coalesce(cast(s.order_external_id AS varchar), ''), '|',
          coalesce(cast(s.tracking_number AS varchar), ''), '|',
          coalesce(cast(s.status AS varchar), ''), '|',
          coalesce(cast(s.weight_grams AS varchar), ''), '|',
          coalesce(cast(s.volume_cubic_cm AS varchar), ''), '|',
          coalesce(cast(s.package_count AS varchar), ''), '|',
          coalesce(cast(s.origin_warehouse_code AS varchar), ''), '|',
          coalesce(cast(s.destination_type AS varchar), ''), '|',
          coalesce(cast(s.destination_pickup_point_code AS varchar), ''), '|',
          coalesce(cast(s.destination_address_external_id AS varchar), ''), '|',
          coalesce(cast(s.created_date AS varchar), ''), '|',
          coalesce(cast(s.dispatched_date AS varchar), ''), '|',
          coalesce(cast(s.estimated_delivery_date AS varchar), ''), '|',
          coalesce(cast(s.actual_delivery_date AS varchar), ''), '|',
          coalesce(cast(s.delivery_notes AS varchar), ''), '|',
          coalesce(cast(s.recipient_name AS varchar), ''), '|',
          coalesce(cast(s.delivery_signature AS varchar), ''), '|',
          coalesce(cast(s.created_at AS varchar), ''), '|',
          coalesce(cast(s.updated_at AS varchar), ''), '|',
          coalesce(cast(s.created_by AS varchar), ''), '|',
          coalesce(cast(s.updated_by AS varchar), '')
        ))))) AS src_hashdiff
      FROM iceberg.logistics_service_db.shipments s
      JOIN iceberg.logistics_service_db.hub_shipment hs
        ON hs.bk_shipment_external_id = s.shipment_external_id
      WHERE s.is_current = true
        AND s.shipment_external_id IS NOT NULL
    ) src
    WHERE src.src_hk_shipment = hk_shipment
      AND src.src_hashdiff <> hashdiff
  );

INSERT INTO iceberg.logistics_service_db.sat_shipment (
  hk_shipment, hashdiff, effective_from, effective_to, is_current, load_dts, record_source,
  order_external_id, tracking_number, status, weight_grams, volume_cubic_cm, package_count, origin_warehouse_code,
  destination_type, destination_pickup_point_code, destination_address_external_id,
  created_date, dispatched_date, estimated_delivery_date, actual_delivery_date,
  delivery_notes, recipient_name, delivery_signature,
  created_at, updated_at, created_by, updated_by
)
SELECT
  src.hk_shipment,
  src.hashdiff,
  cast(current_timestamp AS timestamp(3)),
  TIMESTAMP '9999-12-31 00:00:00.000',
  true,
  src.load_dts,
  src.record_source,
  src.order_external_id,
  src.tracking_number,
  src.status,
  src.weight_grams,
  src.volume_cubic_cm,
  src.package_count,
  src.origin_warehouse_code,
  src.destination_type,
  src.destination_pickup_point_code,
  src.destination_address_external_id,
  src.created_date,
  src.dispatched_date,
  src.estimated_delivery_date,
  src.actual_delivery_date,
  src.delivery_notes,
  src.recipient_name,
  src.delivery_signature,
  src.created_at,
  src.updated_at,
  src.created_by,
  src.updated_by
FROM (
  SELECT
    hs.hk AS hk_shipment,
    lower(to_hex(md5(to_utf8(concat(
      coalesce(cast(s.order_external_id AS varchar), ''), '|',
      coalesce(cast(s.tracking_number AS varchar), ''), '|',
      coalesce(cast(s.status AS varchar), ''), '|',
      coalesce(cast(s.weight_grams AS varchar), ''), '|',
      coalesce(cast(s.volume_cubic_cm AS varchar), ''), '|',
      coalesce(cast(s.package_count AS varchar), ''), '|',
      coalesce(cast(s.origin_warehouse_code AS varchar), ''), '|',
      coalesce(cast(s.destination_type AS varchar), ''), '|',
      coalesce(cast(s.destination_pickup_point_code AS varchar), ''), '|',
      coalesce(cast(s.destination_address_external_id AS varchar), ''), '|',
      coalesce(cast(s.created_date AS varchar), ''), '|',
      coalesce(cast(s.dispatched_date AS varchar), ''), '|',
      coalesce(cast(s.estimated_delivery_date AS varchar), ''), '|',
      coalesce(cast(s.actual_delivery_date AS varchar), ''), '|',
      coalesce(cast(s.delivery_notes AS varchar), ''), '|',
      coalesce(cast(s.recipient_name AS varchar), ''), '|',
      coalesce(cast(s.delivery_signature AS varchar), ''), '|',
      coalesce(cast(s.created_at AS varchar), ''), '|',
      coalesce(cast(s.updated_at AS varchar), ''), '|',
      coalesce(cast(s.created_by AS varchar), ''), '|',
      coalesce(cast(s.updated_by AS varchar), '')
    ))))) AS hashdiff,
    coalesce(s.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
    coalesce(s.__record_source, 'logistics_service_db.shipments') AS record_source,
    s.order_external_id,
    s.tracking_number,
    s.status,
    s.weight_grams,
    s.volume_cubic_cm,
    s.package_count,
    s.origin_warehouse_code,
    s.destination_type,
    s.destination_pickup_point_code,
    s.destination_address_external_id,
    s.created_date,
    s.dispatched_date,
    s.estimated_delivery_date,
    s.actual_delivery_date,
    s.delivery_notes,
    s.recipient_name,
    s.delivery_signature,
    s.created_at,
    s.updated_at,
    s.created_by,
    s.updated_by
  FROM iceberg.logistics_service_db.shipments s
  JOIN iceberg.logistics_service_db.hub_shipment hs
    ON hs.bk_shipment_external_id = s.shipment_external_id
  WHERE s.is_current = true
    AND s.shipment_external_id IS NOT NULL
) src
LEFT JOIN iceberg.logistics_service_db.sat_shipment cur
  ON cur.hk_shipment = src.hk_shipment AND cur.is_current = true
WHERE cur.hk_shipment IS NULL OR cur.hashdiff <> src.hashdiff;

UPDATE iceberg.logistics_service_db.sat_warehouse
SET effective_to = cast(current_timestamp AS timestamp(3)),
    is_current = false
WHERE is_current = true
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT
        hw.hk AS src_hk_warehouse,
        lower(to_hex(md5(to_utf8(concat(
          coalesce(cast(w.warehouse_name AS varchar), ''), '|',
          coalesce(cast(w.warehouse_type AS varchar), ''), '|',
          coalesce(cast(w.country AS varchar), ''), '|',
          coalesce(cast(w.region AS varchar), ''), '|',
          coalesce(cast(w.city AS varchar), ''), '|',
          coalesce(cast(w.street_address AS varchar), ''), '|',
          coalesce(cast(w.postal_code AS varchar), ''), '|',
          coalesce(cast(w.is_active AS varchar), ''), '|',
          coalesce(cast(w.max_capacity_cubic_meters AS varchar), ''), '|',
          coalesce(cast(w.operating_hours AS varchar), ''), '|',
          coalesce(cast(w.contact_phone AS varchar), ''), '|',
          coalesce(cast(w.manager_name AS varchar), ''), '|',
          coalesce(cast(w.created_at AS varchar), ''), '|',
          coalesce(cast(w.updated_at AS varchar), ''), '|',
          coalesce(cast(w.created_by AS varchar), ''), '|',
          coalesce(cast(w.updated_by AS varchar), '')
        ))))) AS src_hashdiff
      FROM iceberg.logistics_service_db.warehouses w
      JOIN iceberg.logistics_service_db.hub_warehouse hw
        ON hw.bk_warehouse_code = w.warehouse_code
      WHERE w.is_current = true
        AND w.warehouse_code IS NOT NULL
    ) src
    WHERE src.src_hk_warehouse = hk_warehouse
      AND src.src_hashdiff <> hashdiff
  );

INSERT INTO iceberg.logistics_service_db.sat_warehouse (
  hk_warehouse, hashdiff, effective_from, effective_to, is_current, load_dts, record_source,
  warehouse_name, warehouse_type, country, region, city, street_address, postal_code,
  is_active, max_capacity_cubic_meters, operating_hours, contact_phone, manager_name,
  created_at, updated_at, created_by, updated_by
)
SELECT
  src.hk_warehouse,
  src.hashdiff,
  cast(current_timestamp AS timestamp(3)),
  TIMESTAMP '9999-12-31 00:00:00.000',
  true,
  src.load_dts,
  src.record_source,
  src.warehouse_name,
  src.warehouse_type,
  src.country,
  src.region,
  src.city,
  src.street_address,
  src.postal_code,
  src.is_active,
  src.max_capacity_cubic_meters,
  src.operating_hours,
  src.contact_phone,
  src.manager_name,
  src.created_at,
  src.updated_at,
  src.created_by,
  src.updated_by
FROM (
  SELECT
    hw.hk AS hk_warehouse,
    lower(to_hex(md5(to_utf8(concat(
      coalesce(cast(w.warehouse_name AS varchar), ''), '|',
      coalesce(cast(w.warehouse_type AS varchar), ''), '|',
      coalesce(cast(w.country AS varchar), ''), '|',
      coalesce(cast(w.region AS varchar), ''), '|',
      coalesce(cast(w.city AS varchar), ''), '|',
      coalesce(cast(w.street_address AS varchar), ''), '|',
      coalesce(cast(w.postal_code AS varchar), ''), '|',
      coalesce(cast(w.is_active AS varchar), ''), '|',
      coalesce(cast(w.max_capacity_cubic_meters AS varchar), ''), '|',
      coalesce(cast(w.operating_hours AS varchar), ''), '|',
      coalesce(cast(w.contact_phone AS varchar), ''), '|',
      coalesce(cast(w.manager_name AS varchar), ''), '|',
      coalesce(cast(w.created_at AS varchar), ''), '|',
      coalesce(cast(w.updated_at AS varchar), ''), '|',
      coalesce(cast(w.created_by AS varchar), ''), '|',
      coalesce(cast(w.updated_by AS varchar), '')
    ))))) AS hashdiff,
    coalesce(w.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
    coalesce(w.__record_source, 'logistics_service_db.warehouses') AS record_source,
    w.warehouse_name,
    w.warehouse_type,
    w.country,
    w.region,
    w.city,
    w.street_address,
    w.postal_code,
    w.is_active,
    w.max_capacity_cubic_meters,
    w.operating_hours,
    w.contact_phone,
    w.manager_name,
    w.created_at,
    w.updated_at,
    w.created_by,
    w.updated_by
  FROM iceberg.logistics_service_db.warehouses w
  JOIN iceberg.logistics_service_db.hub_warehouse hw
    ON hw.bk_warehouse_code = w.warehouse_code
  WHERE w.is_current = true
    AND w.warehouse_code IS NOT NULL
) src
LEFT JOIN iceberg.logistics_service_db.sat_warehouse cur
  ON cur.hk_warehouse = src.hk_warehouse AND cur.is_current = true
WHERE cur.hk_warehouse IS NULL OR cur.hashdiff <> src.hashdiff;

UPDATE iceberg.logistics_service_db.sat_pickup_point
SET effective_to = cast(current_timestamp AS timestamp(3)),
    is_current = false
WHERE is_current = true
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT
        hp.hk AS src_hk_pickup_point,
        lower(to_hex(md5(to_utf8(concat(
          coalesce(cast(p.pickup_point_name AS varchar), ''), '|',
          coalesce(cast(p.pickup_point_type AS varchar), ''), '|',
          coalesce(cast(p.country AS varchar), ''), '|',
          coalesce(cast(p.region AS varchar), ''), '|',
          coalesce(cast(p.city AS varchar), ''), '|',
          coalesce(cast(p.street_address AS varchar), ''), '|',
          coalesce(cast(p.postal_code AS varchar), ''), '|',
          coalesce(cast(p.is_active AS varchar), ''), '|',
          coalesce(cast(p.max_capacity_packages AS varchar), ''), '|',
          coalesce(cast(p.operating_hours AS varchar), ''), '|',
          coalesce(cast(p.contact_phone AS varchar), ''), '|',
          coalesce(cast(p.partner_name AS varchar), ''), '|',
          coalesce(cast(p.created_at AS varchar), ''), '|',
          coalesce(cast(p.updated_at AS varchar), ''), '|',
          coalesce(cast(p.created_by AS varchar), ''), '|',
          coalesce(cast(p.updated_by AS varchar), '')
        ))))) AS src_hashdiff
      FROM iceberg.logistics_service_db.pickup_points p
      JOIN iceberg.logistics_service_db.hub_pickup_point hp
        ON hp.bk_pickup_point_code = p.pickup_point_code
      WHERE p.is_current = true
        AND p.pickup_point_code IS NOT NULL
    ) src
    WHERE src.src_hk_pickup_point = hk_pickup_point
      AND src.src_hashdiff <> hashdiff
  );

INSERT INTO iceberg.logistics_service_db.sat_pickup_point (
  hk_pickup_point, hashdiff, effective_from, effective_to, is_current, load_dts, record_source,
  pickup_point_name, pickup_point_type, country, region, city, street_address, postal_code,
  is_active, max_capacity_packages, operating_hours, contact_phone, partner_name,
  created_at, updated_at, created_by, updated_by
)
SELECT
  src.hk_pickup_point,
  src.hashdiff,
  cast(current_timestamp AS timestamp(3)),
  TIMESTAMP '9999-12-31 00:00:00.000',
  true,
  src.load_dts,
  src.record_source,
  src.pickup_point_name,
  src.pickup_point_type,
  src.country,
  src.region,
  src.city,
  src.street_address,
  src.postal_code,
  src.is_active,
  src.max_capacity_packages,
  src.operating_hours,
  src.contact_phone,
  src.partner_name,
  src.created_at,
  src.updated_at,
  src.created_by,
  src.updated_by
FROM (
  SELECT
    hp.hk AS hk_pickup_point,
    lower(to_hex(md5(to_utf8(concat(
      coalesce(cast(p.pickup_point_name AS varchar), ''), '|',
      coalesce(cast(p.pickup_point_type AS varchar), ''), '|',
      coalesce(cast(p.country AS varchar), ''), '|',
      coalesce(cast(p.region AS varchar), ''), '|',
      coalesce(cast(p.city AS varchar), ''), '|',
      coalesce(cast(p.street_address AS varchar), ''), '|',
      coalesce(cast(p.postal_code AS varchar), ''), '|',
      coalesce(cast(p.is_active AS varchar), ''), '|',
      coalesce(cast(p.max_capacity_packages AS varchar), ''), '|',
      coalesce(cast(p.operating_hours AS varchar), ''), '|',
      coalesce(cast(p.contact_phone AS varchar), ''), '|',
      coalesce(cast(p.partner_name AS varchar), ''), '|',
      coalesce(cast(p.created_at AS varchar), ''), '|',
      coalesce(cast(p.updated_at AS varchar), ''), '|',
      coalesce(cast(p.created_by AS varchar), ''), '|',
      coalesce(cast(p.updated_by AS varchar), '')
    ))))) AS hashdiff,
    coalesce(p.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
    coalesce(p.__record_source, 'logistics_service_db.pickup_points') AS record_source,
    p.pickup_point_name,
    p.pickup_point_type,
    p.country,
    p.region,
    p.city,
    p.street_address,
    p.postal_code,
    p.is_active,
    p.max_capacity_packages,
    p.operating_hours,
    p.contact_phone,
    p.partner_name,
    p.created_at,
    p.updated_at,
    p.created_by,
    p.updated_by
  FROM iceberg.logistics_service_db.pickup_points p
  JOIN iceberg.logistics_service_db.hub_pickup_point hp
    ON hp.bk_pickup_point_code = p.pickup_point_code
  WHERE p.is_current = true
    AND p.pickup_point_code IS NOT NULL
) src
LEFT JOIN iceberg.logistics_service_db.sat_pickup_point cur
  ON cur.hk_pickup_point = src.hk_pickup_point AND cur.is_current = true
WHERE cur.hk_pickup_point IS NULL OR cur.hashdiff <> src.hashdiff;

INSERT INTO iceberg.logistics_service_db.lnk_shipment_order (hk_link, hk_shipment, hk_order, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(concat(hs.hk, '|', ho.hk))))) AS hk_link,
  hs.hk AS hk_shipment,
  ho.hk AS hk_order,
  coalesce(s.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(s.__record_source, 'logistics_service_db.shipments') AS record_source
FROM iceberg.logistics_service_db.shipments s
JOIN iceberg.logistics_service_db.hub_shipment hs
  ON hs.bk_shipment_external_id = s.shipment_external_id
JOIN iceberg.order_service_db.hub_order ho
  ON ho.bk_order_external_id = s.order_external_id
WHERE s.is_current = true
  AND s.shipment_external_id IS NOT NULL
  AND s.order_external_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.logistics_service_db.lnk_shipment_order l
    WHERE l.hk_shipment = hs.hk AND l.hk_order = ho.hk
  );

INSERT INTO iceberg.logistics_service_db.lnk_shipment_warehouse (hk_link, hk_shipment, hk_warehouse, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(concat(hs.hk, '|', hw.hk))))) AS hk_link,
  hs.hk AS hk_shipment,
  hw.hk AS hk_warehouse,
  coalesce(s.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(s.__record_source, 'logistics_service_db.shipments') AS record_source
FROM iceberg.logistics_service_db.shipments s
JOIN iceberg.logistics_service_db.hub_shipment hs
  ON hs.bk_shipment_external_id = s.shipment_external_id
JOIN iceberg.logistics_service_db.hub_warehouse hw
  ON hw.bk_warehouse_code = s.origin_warehouse_code
WHERE s.is_current = true
  AND s.shipment_external_id IS NOT NULL
  AND s.origin_warehouse_code IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.logistics_service_db.lnk_shipment_warehouse l
    WHERE l.hk_shipment = hs.hk AND l.hk_warehouse = hw.hk
  );

INSERT INTO iceberg.logistics_service_db.lnk_shipment_pickup_point (hk_link, hk_shipment, hk_pickup_point, load_dts, record_source)
SELECT DISTINCT
  lower(to_hex(md5(to_utf8(concat(hs.hk, '|', hp.hk))))) AS hk_link,
  hs.hk AS hk_shipment,
  hp.hk AS hk_pickup_point,
  coalesce(s.__ingest_dts, cast(current_timestamp AS timestamp(3))) AS load_dts,
  coalesce(s.__record_source, 'logistics_service_db.shipments') AS record_source
FROM iceberg.logistics_service_db.shipments s
JOIN iceberg.logistics_service_db.hub_shipment hs
  ON hs.bk_shipment_external_id = s.shipment_external_id
JOIN iceberg.logistics_service_db.hub_pickup_point hp
  ON hp.bk_pickup_point_code = s.destination_pickup_point_code
WHERE s.is_current = true
  AND s.shipment_external_id IS NOT NULL
  AND s.destination_pickup_point_code IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.logistics_service_db.lnk_shipment_pickup_point l
    WHERE l.hk_shipment = hs.hk AND l.hk_pickup_point = hp.hk
  );