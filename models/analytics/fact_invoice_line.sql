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


SELECT *
FROM fact_invoice_line__rename_column