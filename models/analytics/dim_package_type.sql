WITH
  dim_package_type__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)

, dim_package_type__rename_column AS (
    SELECT
      package_type_id AS package_type_key
      , package_type_name
    FROM dim_package_type__source
)


SELECT *
FROM dim_package_type__rename_column