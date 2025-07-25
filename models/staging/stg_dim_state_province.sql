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


SELECT *
FROM dim_state_province__rename_column