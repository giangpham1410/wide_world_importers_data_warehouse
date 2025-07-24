WITH
  dim_customer__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS (
    SELECT
      customer_id AS customer_key
      , customer_category_id AS customer_category_key
      , buying_group_id AS buying_group_key
      , customer_name
      , is_on_credit_hold AS is_on_credit_hold_boolean
    FROM dim_customer__source
)

, dim_customer__cast_type AS (
    SELECT
      CAST(customer_key AS INTEGER) customer_key
      , CAST(customer_category_key AS INTEGER) AS customer_category_key
      , CAST(buying_group_key AS INTEGER) AS buying_group_key
      , CAST(customer_name AS STRING) customer_name
      , CAST(is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean
    FROM dim_customer__rename_column
)

, dim_customer__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
          WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
          WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_on_credit_hold
    FROM dim_customer__cast_type
)

, dim_customer__handle_null AS (
    SELECT
      customer_key
      , customer_name
      , COALESCE(customer_category_key, 0) AS customer_category_key
      , COALESCE(buying_group_key, 0) AS buying_group_key
      , is_on_credit_hold
    FROM dim_customer__convert_boolean
)

, dim_customer__add_undefined_record AS (
    SELECT
      customer_key
      , customer_name
      , customer_category_key
      , buying_group_key
      , is_on_credit_hold
    FROM dim_customer__handle_null

    UNION ALL
    SELECT
      0 AS customer_key
      , 'Undefined' AS customer_name
      , 0 AS customer_category_key
      , 0 AS buying_group_key
      , 'Undefined' AS is_on_credit_hold

    UNION ALL
    SELECT
      -1 AS customer_key
      , 'Invalid' AS customer_name
      , -1 AS customer_category_key
      , -1 AS buying_group_key
      , 'Invalid' AS is_on_credit_hold
)


SELECT
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_on_credit_hold

  , dim_customer.customer_category_key
  , COALESCE(dim_customer_category.customer_category_name, 'Invalid') AS customer_category_name
  , dim_customer.buying_group_key
  , COALESCE(dim_buying_group.buying_group_name, 'Invalid') AS buying_group_name
FROM dim_customer__add_undefined_record dim_customer
  LEFT JOIN {{ ref('stg_dim_customer_category') }} dim_customer_category
    ON dim_customer.customer_category_key = dim_customer_category.customer_category_key
  LEFT JOIN {{ ref('stg_dim_buying_group') }} dim_buying_group
    ON dim_customer.buying_group_key = dim_buying_group.buying_group_key