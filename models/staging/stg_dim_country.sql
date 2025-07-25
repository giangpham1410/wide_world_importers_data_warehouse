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

, dim_country__cast_type AS (
    SELECT
      CAST(country_key AS INTEGER) AS country_key
      , CAST(country_name AS STRING) AS country_name
      , CAST(country_code AS STRING) AS country_code
      , CAST(region AS STRING) AS region
    FROM dim_country__rename_column
)

, dim_country__handle_null AS (
    SELECT
      country_key
      , country_name
      , COALESCE(country_code, 'Undefined') AS country_code
      , region
    FROM dim_country__cast_type
)

, dim_country__add_undefined_record AS (
    SELECT
      country_key
      , country_name
      , country_code
      , region
    FROM dim_country__handle_null

    UNION ALL
    SELECT
      0 AS country_key
      , 'Undefined' AS country_name
      , 'Undefined' AS country_code
      , 'Undefined' AS region
    
    UNION ALL
    SELECT
      -1 AS country_key
      , 'Invalid' AS country_name
      , 'Invalid' AS country_code
      , 'Invalid' AS region
)


SELECT
  country_key
  , country_name
  , country_code
  , region
FROM dim_country__add_undefined_record
