WITH
  dim_person__source AS (
    SELECT 
      *
    FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS (
    SELECT
      person_id AS person_key
      , full_name AS full_name
      , preferred_name
      , is_employee AS is_employee_boolean
      , is_salesperson AS is_salesperson_boolean
      , is_external_logon_provider AS is_external_logon_provider_boolean
    FROM dim_person__source
)

, dim_person__cast_type AS (
    SELECT
      CAST(person_key AS INTEGER) AS person_key
      , CAST(full_name AS STRING) AS full_name
      , CAST(preferred_name AS STRING) AS preferred_name
      , CAST(is_employee_boolean AS BOOLEAN) AS is_employee_boolean
      , CAST(is_salesperson_boolean AS BOOLEAN) AS is_salesperson_boolean
      , CAST(is_external_logon_provider_boolean AS BOOLEAN) AS is_external_logon_provider_boolean
    FROM dim_person__rename_column
)

, dim_person__convert_boolean AS (
    SELECT
      person_key
      , full_name
      , preferred_name
      , CASE
          WHEN is_employee_boolean IS TRUE THEN 'Employee'
          WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
          WHEN is_employee_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
          END AS is_employee
      , CASE
          WHEN is_salesperson_boolean IS TRUE THEN 'Salesperson'
          WHEN is_salesperson_boolean IS FALSE THEN 'Not Salesperson'
          WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
          END AS is_salesperson
      , CASE
          WHEN is_external_logon_provider_boolean IS TRUE THEN 'External Logon Provider'
          WHEN is_external_logon_provider_boolean IS FALSE THEN 'Not External Logon Provider'
          WHEN is_external_logon_provider_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
          END AS is_external_logon_provider
    FROM dim_person__cast_type
)

, dim_person__add_undefined_record AS (
    SELECT
      person_key
      , full_name
      , preferred_name
      , is_employee
      , is_salesperson
      , is_external_logon_provider
    FROM dim_person__convert_boolean

    UNION ALL
    SELECT
      0 AS person_key
      , 'Undefined' AS full_name
      , 'Undefined' AS preferred_name
      , 'Undefined' AS is_employee
      , 'Undefined' AS is_salesperson
      , 'Undefined' AS is_external_logon_provider

    UNION ALL
    SELECT
      -1 AS person_key
      , 'Invalid' AS full_name
      , 'Invalid' AS preferred_name
      , 'Invalid' AS is_employee
      , 'Invalid' AS is_salesperson
      , 'Invalid' AS is_external_logon_provider
)


SELECT
  person_key
  , full_name
  , preferred_name
  , is_employee
  , is_salesperson
  , is_external_logon_provider
FROM dim_person__add_undefined_record
