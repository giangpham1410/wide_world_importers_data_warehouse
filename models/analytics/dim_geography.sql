WITH
  dim_geography__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__cities`
)

, dim_geography__rename_column AS (
    SELECT
      city_id AS city_key
      , city_name
      , state_province_id AS state_province_key
    FROM dim_geography__source
)

, dim_geography__cast_type AS (
    SELECT
      CAST(city_key AS INTEGER) AS city_key
      , CAST(city_name AS STRING) AS city_name
      , CAST(state_province_key AS INTEGER) AS state_province_key
    FROM dim_geography__rename_column
)

, dim_geography__add_undefined_record AS (
    SELECT
      city_key
      , city_name
      , state_province_key
    FROM dim_geography__cast_type

    UNION ALL
    SELECT
        0 AS city_key
      , 'Undefined' AS city_name
      , 0 AS state_province_key

    UNION ALL
    SELECT
        -1 AS city_key
      , 'Invalid' AS city_name
      , -1 AS state_province_key
)


SELECT
  city_key
  , city_name
  , state_province_key
FROM dim_geography__add_undefined_record
