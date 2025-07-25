WITH
  dim_country__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__countries`
)

, dim_country__rename_column AS (
    SELECT
      country_id AS country_key
      , country_name
      , iso_alpha_3_code AS country_code
      , region
    FROM dim_country__source
)


SELECT *
FROM dim_country__rename_column