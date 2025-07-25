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

, dim_state_province__add_undefined_record AS (
    SELECT
      state_province_key
      , state_province_name
      , state_province_code
      , sales_territory
      , country_key
    FROM dim_state_province__cast_type

    UNION ALL
    SELECT
      0 AS state_province_key
      , 'Undefined' AS state_province_name
      , 'Undefined' AS state_province_code
      , 'Undefined' AS sales_territory
      , 0 AS country_key

    UNION ALL
    SELECT
      -1 AS state_province_key
      , 'Invalid' AS state_province_name
      , 'Invalid' AS state_province_code
      , 'Invalid' AS sales_territory
      , -1 AS country_key
)


SELECT
  -- PROVINCE
    dim_state_province.state_province_key
  , dim_state_province.state_province_name
  , dim_state_province.state_province_code
  , dim_state_province.sales_territory

  -- COUNTRY
  , dim_state_province.country_key
  , COALESCE(dim_country.country_name, 'Invalid') AS country_name
  , COALESCE(dim_country.country_code, 'Invalid') AS country_code
  , COALESCE(dim_country.region, 'Invalid') AS region
FROM dim_state_province__add_undefined_record AS dim_state_province
  LEFT JOIN {{ ref('stg_dim_country') }} AS dim_country
    ON dim_state_province.country_key = dim_country.country_key