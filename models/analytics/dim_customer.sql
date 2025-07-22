WITH
  dim_customer__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS (
    SELECT
      customer_id AS customer_key
      , customer_name
    FROM dim_customer__source
)


SELECT 
  *
FROM dim_customer__rename_column