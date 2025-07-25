WITH
  dim_state_province__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, dim_state_province__rename_column AS (
    SELECT
      state_province_id AS state_province_key
      , state_province_name
      , state_province_code
      , sales_territory
      , country_id AS country_key
    FROM dim_state_province__source
)

, dim_state_province__cast_type AS (
    SELECT
      CAST(state_province_key AS INTEGER) AS state_province_key
      , CAST(state_province_name AS STRING) AS state_province_name
      , CAST(state_province_code AS STRING) AS state_province_code
      , CAST(sales_territory AS STRING) AS sales_territory
      , CAST(country_key AS INTEGER) AS country_key
    FROM dim_state_province__rename_column
)


SELECT *
FROM dim_state_province__cast_type