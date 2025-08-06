WITH
  fact_invoice_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__invoice_lines`
)

, fact_invoice_line__rename_column AS (
    SELECT
      invoice_line_id AS invoice_line_key
      , description
      , invoice_id AS invoice_key
      , stock_item_id AS product_key
      , package_type_id AS package_type_key
      , quantity
      , unit_price
      , tax_rate
      , tax_amount
      , line_profit
      , extended_price
    FROM fact_invoice_line__source
)

, fact_invoice_line__cast_type AS (
    SELECT
      CAST(invoice_line_key AS INTEGER) AS invoice_line_key
      , CAST(description AS STRING) AS description
      , CAST(invoice_key AS INTEGER) AS invoice_key
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(package_type_key AS INTEGER) AS package_type_key
      , CAST(quantity AS INTEGER) AS quantity
      , CAST(unit_price AS NUMERIC) AS unit_price
      , CAST(tax_rate AS NUMERIC) AS tax_rate
      , CAST(tax_amount AS NUMERIC) AS tax_amount
      , CAST(line_profit AS NUMERIC) AS line_profit
      , CAST(extended_price AS NUMERIC) AS extended_price
    FROM fact_invoice_line__rename_column
)

, fact_invoice_line__handle_null AS (
    SELECT
      invoice_line_key
      , description
      , COALESCE(invoice_key, 0) AS invoice_key
      , COALESCE(product_key, 0) AS product_key
      , COALESCE(package_type_key, 0) AS package_type_key
      , quantity
      , unit_price
      , tax_rate
      , tax_amount
      , line_profit
      , extended_price
    FROM fact_invoice_line__cast_type
)


SELECT
    fact_invoice_line.invoice_line_key
  , fact_invoice_line.description

  -- Invoice Header Information
  , fact_invoice_header.is_credit_note_boolean
  , fact_invoice_header.credit_note_reason
  , fact_invoice_header.customer_purchase_order_number
  , fact_invoice_header.invoice_date
  , fact_invoice_header.confirmed_delivery_at
  , fact_invoice_header.confirmed_received_by
  , fact_invoice_header.total_dry_items
  , fact_invoice_header.total_chiller_items
  , fact_invoice_header.received_by
  , fact_invoice_header.con_note
  , fact_invoice_header.status
  , fact_invoice_header.delivered_at

  -- Invoice Line - FK 
  , fact_invoice_line.invoice_key
  , fact_invoice_line.product_key
  , fact_invoice_line.package_type_key

  -- Invoice Header - FK
  , COALESCE(fact_invoice_header.sales_order_key, -1) AS sales_order_key
  , COALESCE(fact_invoice_header.customer_key, -1) AS customer_key
  , COALESCE(fact_invoice_header.bill_to_customer_key, -1) AS bill_to_customer_key
  , COALESCE(fact_invoice_header.delivery_method_key, -1) AS delivery_method_key
  , COALESCE(fact_invoice_header.contact_person_Key, -1) AS contact_person_Key
  , COALESCE(fact_invoice_header.accounts_person_key, -1) AS accounts_person_key
  , COALESCE(fact_invoice_header.salesperson_person_key, -1) AS salesperson_person_key
  , COALESCE(fact_invoice_header.packed_by_person_key, -1) AS packed_by_person_key

  -- Indicator FK
  , FARM_FINGERPRINT(
    CONCAT(
      is_credit_note_boolean
      , ','
      , package_type_key
    )
  ) AS invoice_line_indicator_key

  -- Measure
  , fact_invoice_line.quantity
  , fact_invoice_line.unit_price
  , fact_invoice_line.tax_rate
  , fact_invoice_line.tax_amount
  , fact_invoice_line.line_profit
  , fact_invoice_line.extended_price
FROM fact_invoice_line__handle_null AS fact_invoice_line
  LEFT JOIN {{ ref('stg_fact_invoice') }} AS fact_invoice_header
    ON fact_invoice_line.invoice_key = fact_invoice_header.invoice_key