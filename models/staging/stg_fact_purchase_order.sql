WITH
  stg_fact_purchase_order__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
)

, stg_fact_purchase_order__rename_column AS (
    SELECT
      purchase_order_id AS purchase_order_key
      , is_order_finalized AS is_order_finalized_boolean
      , order_date
      , expected_delivery_date
      , supplier_id AS supplier_key
      , delivery_method_id AS delivery_method_key
      , contact_person_id AS contact_person_Key
    FROM stg_fact_purchase_order__source
)

, stg_fact_purchase_order__cast_type AS (
    SELECT
      CAST(purchase_order_key AS INTEGER) AS purchase_order_key
      , CAST(is_order_finalized_boolean AS BOOLEAN) AS is_order_finalized_boolean
      , CAST(order_date AS DATE) AS order_date
      , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(delivery_method_key AS INTEGER) AS delivery_method_key
      , CAST(contact_person_Key AS INTEGER) AS contact_person_Key
    FROM stg_fact_purchase_order__rename_column
)

, stg_fact_purchase_order__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_order_finalized_boolean IS TRUE THEN 'Order Finalized'
          WHEN is_order_finalized_boolean IS FALSE THEN 'Order Not Finalized'
          WHEN is_order_finalized_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_order_finalized
    FROM stg_fact_purchase_order__cast_type
)

SELECT
  purchase_order_key
  , is_order_finalized
  , order_date
  , expected_delivery_date
  , supplier_key
  , delivery_method_key
  , contact_person_Key
FROM stg_fact_purchase_order__convert_boolean