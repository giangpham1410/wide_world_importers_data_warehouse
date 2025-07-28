WITH
  fact_sales_order__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order__rename_column AS (
    SELECT
      order_id AS sales_order_key
      , is_undersupply_backordered AS is_undersupply_backordered_boolean
      , customer_purchase_order_number
      , order_date
      , picking_completed_when AS so_picking_completed_at
      , expected_delivery_date
      , customer_id AS customer_key
      , picked_by_person_id AS picked_by_person_key
      , contact_person_id AS contact_person_key
      , salesperson_person_id AS salesperson_person_key
      , backorder_order_id AS backorder_order_key
    FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
    SELECT
      CAST(sales_order_key AS INTEGER) AS sales_order_key
      , CAST(order_date AS DATE) AS order_date
      , CAST(customer_key AS INTEGER) AS customer_key
      , CAST(picked_by_person_key AS INTEGER) AS picked_by_person_key
    FROM fact_sales_order__rename_column
)

, fact_sales_order__handle_null AS (
    SELECT
      sales_order_key
      , customer_key
      , order_date
      , COALESCE(picked_by_person_key, 0) AS picked_by_person_key
    FROM fact_sales_order__cast_type
)


SELECT *
FROM fact_sales_order__rename_column