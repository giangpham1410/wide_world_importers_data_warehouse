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

, fact_purchase_order_line__cast_type AS (
    SELECT
      CAST(purchase_order_line_key AS INTEGER) AS purchase_order_line_key
      , CAST(description AS STRING) AS description
      , CAST(is_order_line_finalized_boolean AS BOOLEAN) AS is_order_line_finalized_boolean
      , CAST(last_receipt_date AS DATE) AS last_receipt_date
      , CAST(purchase_order_key AS INTEGER) AS purchase_order_key
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(package_type_key AS INTEGER) AS package_type_key
      , CAST(ordered_outers AS INTEGER) AS ordered_outers
      , CAST(received_outers AS INTEGER) AS received_outers
      , CAST(expected_unit_price_per_outer AS INTEGER) AS expected_unit_price_per_outer
    FROM fact_purchase_order_line__rename_column
)

, fact_purchase_order_line__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_order_line_finalized_boolean IS TRUE THEN 'Order Line Finalized'
          WHEN is_order_line_finalized_boolean IS FALSE THEN 'Order Line Not Finalized'
          WHEN is_order_line_finalized_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_order_line_finalized
    FROM fact_purchase_order_line__cast_type
)

SELECT
  fact_po_line.purchase_order_line_key
  , fact_po_line.description
  , fact_po_line.is_order_line_finalized
  , fact_po_header.is_order_finalized
  
  -- DATE
  , fact_po_header.order_date
  , fact_po_header.expected_delivery_date
  , fact_po_line.last_receipt_date

  -- FK
  , fact_po_line.purchase_order_key
  , fact_po_line.product_key
  , fact_po_line.package_type_key
  , fact_po_header.supplier_key
  , fact_po_header.delivery_method_key
  , fact_po_header.contact_person_Key

  -- MEASURE
  , fact_po_line.ordered_outers
  , fact_po_line.received_outers
  , fact_po_line.expected_unit_price_per_outer
FROM fact_purchase_order_line__convert_boolean AS fact_po_line
  LEFT JOIN {{ ref('stg_fact_purchase_order') }} AS fact_po_header
    ON fact_po_line.purchase_order_key = fact_po_header.purchase_order_key