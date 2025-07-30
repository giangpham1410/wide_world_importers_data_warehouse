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
      , quantity AS quantity_sold
      , unit_price
      , description
      , picking_completed_when AS so_line_picking_completed_at
      , package_type_id AS package_type_key
      , tax_rate
      , picked_quantity
    FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS (
    SELECT
      CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
      , CAST(sales_order_key AS INTEGER) AS sales_order_key
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(quantity_sold AS INTEGER) AS quantity_sold
      , CAST(unit_price AS NUMERIC) AS unit_price
      , CAST(description AS STRING) AS description
      , CAST(so_line_picking_completed_at AS DATETIME) AS so_line_picking_completed_at
      , CAST(package_type_key AS INTEGER) AS package_type_key
      , CAST(tax_rate AS NUMERIC) AS tax_rate
      , CAST(picked_quantity AS INTEGER) AS picked_quantity
    FROM fact_sales_order_line__rename_column
)

, fact_sales_order_line__calculate_measure AS (
    SELECT
      *
      , unit_price * quantity_sold AS gross_amount
      , unit_price * quantity_sold * tax_rate/100 AS tax_amount -- gross_amount * tax_rate
      , (unit_price * quantity_sold) - (unit_price * quantity_sold * tax_rate/100) AS net_amount -- gross_amount - tax_amount
    FROM fact_sales_order_line__cast_type
)

SELECT
  fact_so_line.sales_order_line_key
  , fact_so_line.description
  --, COALESCE(fact_so_header.is_undersupply_backordered, 'Undefined') AS is_undersupply_backordered
  , fact_so_header.customer_purchase_order_number
  , fact_so_header.order_date
  , fact_so_header.expected_delivery_date
  , fact_so_header.so_picking_completed_at
  , fact_so_line.so_line_picking_completed_at
  
  -- FOREIGN KEY
  , fact_so_line.sales_order_key
  , fact_so_line.product_key
  --, fact_so_line.package_type_key

  -- Handle NULL khi fact_so_line load full du lieu nhung fact_so_header load thieu du lieu
  , COALESCE(fact_so_header.customer_key, -1) AS customer_key
  , COALESCE(fact_so_header.salesperson_person_key, -1) AS salesperson_person_key
  , COALESCE(fact_so_header.picked_by_person_key, -1) AS picked_by_person_key
  , COALESCE(fact_so_header.contact_person_key, -1) AS contact_person_key
  , COALESCE(fact_so_header.backorder_order_key, -1) AS backorder_order_key

  -- COMPOSITE KEY
  , CONCAT(
      fact_so_header.is_undersupply_backordered_boolean
      , ','
      , fact_so_line.package_type_key
      ) AS sales_order_line_indicator_key
  
  -- FACT
  , fact_so_line.unit_price
  , fact_so_line.quantity_sold
  , fact_so_line.tax_rate
  , fact_so_line.picked_quantity

  -- CALCULATED MEASURE
  , fact_so_line.gross_amount
  , fact_so_line.tax_amount
  , fact_so_line.net_amount
FROM fact_sales_order_line__calculate_measure AS fact_so_line
  LEFT JOIN {{ ref('stg_fact_sales_order') }} AS fact_so_header
    ON fact_so_line.sales_order_key = fact_so_header.sales_order_key
    