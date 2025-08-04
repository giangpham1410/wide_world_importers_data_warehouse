WITH
  fact_purchase_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`
)

, fact_purchase_order_line__rename_column AS (
    SELECT
      purchase_order_line_id AS purchase_order_line_key
      , description
      , is_order_line_finalized AS is_order_line_finalized_boolean
      , last_receipt_date
      , purchase_order_id AS purchase_order_key
      , stock_item_id AS product_key
      , package_type_id AS package_type_key
      , ordered_outers
      , received_outers
      , expected_unit_price_per_outer
    FROM fact_purchase_order_line__source
)

SELECT *
FROM fact_purchase_order_line__rename_column