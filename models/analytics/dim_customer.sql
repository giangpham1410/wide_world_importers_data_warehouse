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
      , is_statement_sent AS is_statement_sent_boolean
      , phone_number
      , credit_limit
      , standard_discount_percentage AS standard_discount_pct
      , payment_days
      , account_opened_date
      , bill_to_customer_id AS bill_to_customer_key
      , primary_contact_person_id AS primary_contact_person_key
      , alternate_contact_person_id AS alternate_contact_person_key
      , delivery_method_id AS delivery_method_key
      , delivery_city_id AS delivery_city_key
      , postal_city_id AS postal_city_key
    FROM dim_customer__source
)

, dim_customer__cast_type AS (
    SELECT
      CAST(customer_key AS INTEGER) customer_key
      , CAST(customer_category_key AS INTEGER) AS customer_category_key
      , CAST(buying_group_key AS INTEGER) AS buying_group_key
      , CAST(customer_name AS STRING) customer_name
      , CAST(is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean
      , CAST(is_statement_sent_boolean AS BOOLEAN) AS is_statement_sent_boolean
      , CAST(phone_number AS STRING) AS phone_number
      , CAST(credit_limit AS NUMERIC) AS credit_limit
      , CAST(standard_discount_pct AS NUMERIC) AS standard_discount_pct
      , CAST(payment_days AS INTEGER) AS payment_days
      , CAST(account_opened_date AS DATE) AS account_opened_date
      , CAST(bill_to_customer_key AS INTEGER) AS bill_to_customer_key
      , CAST(primary_contact_person_key AS INTEGER) AS primary_contact_person_key
      , CAST(alternate_contact_person_key AS INTEGER) AS alternate_contact_person_key
      , CAST(delivery_method_key AS INTEGER) AS delivery_method_key
      , CAST(delivery_city_key AS INTEGER) AS delivery_city_key
      , CAST(postal_city_key AS INTEGER) AS postal_city_key
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
      , CASE
          WHEN is_statement_sent_boolean IS TRUE THEN 'Statement Sent'
          WHEN is_statement_sent_boolean IS FALSE THEN 'Not Statement Sent'
          WHEN is_statement_sent_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
          END AS is_statement_sent
    FROM dim_customer__cast_type
)

-- Xử lý NULL ở những cột gốc không nhập dữ liệu
, dim_customer__handle_null AS (
    SELECT
      customer_key
      , customer_name
      , COALESCE(customer_category_key, 0) AS customer_category_key
      , COALESCE(buying_group_key, 0) AS buying_group_key
      , is_on_credit_hold
      , is_statement_sent
      , phone_number
      , credit_limit
      , standard_discount_pct
      , payment_days
      , account_opened_date
      , bill_to_customer_key
      , primary_contact_person_key
      , COALESCE(alternate_contact_person_key, 0) AS alternate_contact_person_key
      , COALESCE(delivery_method_key, 0) AS delivery_method_key
      , COALESCE(delivery_city_key, 0) AS delivery_city_key
      , COALESCE(postal_city_key, 0) AS postal_city_key
    FROM dim_customer__convert_boolean
)

, dim_customer__add_undefined_record AS (
    SELECT
      customer_key
      , customer_name
      , customer_category_key
      , buying_group_key
      , is_on_credit_hold
      , is_statement_sent
      , phone_number
      , credit_limit
      , standard_discount_pct
      , payment_days
      , account_opened_date
      , bill_to_customer_key
      , primary_contact_person_key
      , alternate_contact_person_key
      , delivery_method_key
      , delivery_city_key
      , postal_city_key
    FROM dim_customer__handle_null

    UNION ALL
    SELECT
      0 AS customer_key
      , 'Undefined' AS customer_name
      , 0 AS customer_category_key
      , 0 AS buying_group_key
      , 'Undefined' AS is_on_credit_hold
      , 'Undefined' AS is_statement_sent
      , 'Undefined' AS phone_number
      , NULL AS credit_limit
      , NULL AS standard_discount_pct
      , NULL AS payment_days
      , NULL AS account_opened_date
      , 0 AS bill_to_customer_key
      , 0 AS primary_contact_person_key
      , 0 AS alternate_contact_person_key
      , 0 AS delivery_method_key
      , 0 AS delivery_city_key
      , 0 AS postal_city_key

    UNION ALL
    SELECT
      -1 AS customer_key
      , 'Invalid' AS customer_name
      , -1 AS customer_category_key
      , -1 AS buying_group_key
      , 'Invalid' AS is_on_credit_hold
      , 'Invalid' AS is_statement_sent
      , 'Invalid' AS phone_number
      , NULL AS credit_limit
      , NULL AS standard_discount_pct
      , NULL AS payment_days
      , NULL AS account_opened_date
      , -1 AS bill_to_customer_key
      , -1 AS primary_contact_person_key
      , -1 AS alternate_contact_person_key
      , -1 AS delivery_method_key
      , -1 AS delivery_city_key
      , -1 AS postal_city_key
)

SELECT
  -- CUSTOMER
    dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_on_credit_hold
  , dim_customer.is_statement_sent
  , dim_customer.phone_number
  , dim_customer.account_opened_date
  , dim_customer.credit_limit
  , dim_customer.standard_discount_pct
  , dim_customer.payment_days

  -- CUSTOMER CATEGORY
  , dim_customer.customer_category_key
  , dim_customer_category.customer_category_name
  
  -- BUYING GROUP
  , dim_customer.buying_group_key
  , dim_buying_group.buying_group_name

  -- CONTACT
  , dim_customer.bill_to_customer_key
  , dim_customer.primary_contact_person_key
  , dim_customer.alternate_contact_person_key

  -- DELIVERY METHOD
  , dim_customer.delivery_method_key
  , dim_delivery_method.delivery_method_name

  -- CITY
  , dim_customer.delivery_city_key
  , dim_customer.postal_city_key
FROM dim_customer__add_undefined_record dim_customer
  LEFT JOIN {{ ref('stg_dim_customer_category') }} AS dim_customer_category
    ON dim_customer.customer_category_key = dim_customer_category.customer_category_key
  LEFT JOIN {{ ref('stg_dim_buying_group') }} AS dim_buying_group
    ON dim_customer.buying_group_key = dim_buying_group.buying_group_key
  LEFT JOIN {{ ref('dim_delivery_method') }} AS dim_delivery_method
    ON dim_customer.delivery_method_key = dim_delivery_method.delivery_method_key

/*
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
*/