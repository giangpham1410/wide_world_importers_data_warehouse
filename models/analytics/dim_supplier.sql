WITH 
  dim_supplier__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column AS (
    SELECT
      supplier_id AS supplier_key
      , supplier_name
    FROM dim_supplier__source
)


SELECT *
FROM dim_supplier__rename_column
