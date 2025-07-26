WITH
  dim_supplier_category__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
)

, dim_supplier_category__rename_column AS (
    SELECT
      supplier_category_id AS supplier_category_key
      , supplier_category_name
    FROM dim_supplier_category__source
)


SELECT *
FROM dim_supplier_category__rename_column