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


SELECT
  invoice_line_key
  , description
  , invoice_key
  , product_key
  , package_type_key
  , quantity
  , unit_price
  , tax_rate
  , tax_amount
  , line_profit
  , extended_price
FROM fact_invoice_line__cast_type