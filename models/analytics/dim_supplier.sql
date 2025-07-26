WITH 
  dim_supplier__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column AS (
    SELECT
      supplier_id AS supplier_key
      , supplier_name
      , supplier_category_id AS supplier_category_key
      , delivery_method_id AS delivery_method_key
      , delivery_city_id AS delivery_city_key
      , postal_city_id AS postal_city_key
      , primary_contact_person_id AS primary_contact_person_key
      , payment_days
    FROM dim_supplier__source
)

, dim_supplier__cast_type AS (
    SELECT
      CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(supplier_name AS STRING) AS supplier_name
      , CAST(supplier_category_key AS INTEGER) AS supplier_category_key
      , CAST(delivery_method_key AS INTEGER) AS delivery_method_key
      , CAST(delivery_city_key AS INTEGER) AS delivery_city_key
      , CAST(postal_city_key AS INTEGER) AS postal_city_key
      , CAST(primary_contact_person_key AS INTEGER) AS primary_contact_person_key
      , CAST(payment_days AS INTEGER) AS payment_days
    FROM dim_supplier__rename_column
)

, dim_supplier__handle_null AS (
    SELECT
      supplier_key
      , supplier_name
      , COALESCE(supplier_category_key, 0) AS supplier_category_key
      , COALESCE(delivery_method_key, 0) AS delivery_method_key
      , COALESCE(delivery_city_key, 0) AS delivery_city_key
      , COALESCE(postal_city_key, 0) AS postal_city_key
      , COALESCE(primary_contact_person_key, 0) AS primary_contact_person_key
      , payment_days
    FROM dim_supplier__cast_type
)

, dim_supplier__add_undefined_record AS (
    SELECT
      supplier_key
      , supplier_name
      , supplier_category_key
      , delivery_method_key
      , delivery_city_key
      , postal_city_key
      , primary_contact_person_key
      , payment_days
    FROM dim_supplier__handle_null

    UNION ALL
    SELECT
      0 AS supplier_key
      , 'Undefined' AS supplier_name
      , 0 AS supplier_category_key
      , 0 AS delivery_method_key
      , 0 AS delivery_city_key
      , 0 AS postal_city_key
      , 0 AS primary_contact_person_key
      , NULL AS payment_days

    UNION ALL
    SELECT
      -1 AS supplier_key
      , 'Invalid' AS supplier_name
      , -1 AS supplier_category_key
      , -1 AS delivery_method_key
      , -1 AS delivery_city_key
      , -1 AS postal_city_key
      , -1 AS primary_contact_person_key
      , NULL AS payment_days

)

SELECT
  supplier_key
  , supplier_name
  , supplier_category_key
  , delivery_method_key
  , delivery_city_key
  , postal_city_key
  , primary_contact_person_key
FROM dim_supplier__add_undefined_record
