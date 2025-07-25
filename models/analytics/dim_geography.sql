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
  -- CITY
    dim_geography.city_key
  , dim_geography.city_name

  -- PROVINCE
  , dim_geography.state_province_key
  , COALESCE(dim_state_province.state_province_name, 'Invalid') AS state_province_name
  , COALESCE(dim_state_province.state_province_code, 'Invalid') AS state_province_code
  , COALESCE(dim_state_province.sales_territory, 'Invalid') AS sales_territory

  -- COUNTRY
  , COALESCE(dim_state_province.country_key, -1) AS country_key
  , COALESCE(dim_state_province.country_name, 'Invalid') AS country_name
  , COALESCE(dim_state_province.country_code, 'Invalid') AS country_code
  , COALESCE(dim_state_province.region, 'Invalid') AS region
FROM dim_geography__add_undefined_record AS dim_geography
  LEFT JOIN {{ ref('stg_dim_state_province') }} AS dim_state_province
    ON dim_geography.state_province_key = dim_state_province.state_province_key
