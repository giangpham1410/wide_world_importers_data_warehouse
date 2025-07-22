WITH
  fact_sales_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__order_lines`
  )

, fact_sales_order_line__rename_column AS (
    SELECT
      order_line_id AS sales_order_line_key
      , order_id AS sales_order_key
      , stock_item_id AS product_key
      , quantity
      , unit_price
    FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS (
    SELECT
      CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
      , CAST(sales_order_key AS INTEGER) AS sales_order_key
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(quantity AS INTEGER) AS quantity
      , CAST(unit_price AS NUMERIC) AS unit_price
    FROM fact_sales_order_line__rename_column
)

SELECT 
    fact_so_line.sales_order_line_key
  , fact_so_line.sales_order_key
  , fact_so_header.customer_key
  , fact_so_line.product_key
  , fact_so_line.quantity
  , fact_so_line.unit_price
  , fact_so_line.quantity * fact_so_line.unit_price AS gross_amount
FROM fact_sales_order_line__cast_type fact_so_line
  LEFT JOIN `wide-world-importers-dwh.wide_world_importers_dwh_staging.stg_fact_sales_order` fact_so_header ON fact_so_line.sales_order_key = fact_so_header.sales_order_key
