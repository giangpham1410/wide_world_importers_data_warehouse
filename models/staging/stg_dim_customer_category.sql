WITH
  dim_customer_category__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customer_categories`
)

, dim_customer_category__rename_column AS (
    SELECT
      customer_category_id AS customer_category_key
      , customer_category_name
    FROM dim_customer_category__source
)


SELECT *
FROM dim_customer_category__rename_column