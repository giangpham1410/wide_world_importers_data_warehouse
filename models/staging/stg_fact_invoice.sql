WITH
  fact_invoice__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__invoices`
)

, fact_invoice__rename_column AS (
    SELECT
      invoice_id AS invoice_key
      , is_credit_note AS is_credit_note_boolean
      , credit_note_reason
      , customer_purchase_order_number
      , invoice_date
      , confirmed_delivery_time AS confirmed_delivery_at
      , confirmed_received_by
      , returned_delivery_data
      , total_dry_items
      , total_chiller_items
      , order_id AS sales_order_key
      , customer_id AS customer_key
      , bill_to_customer_id AS bill_to_customer_key
      , delivery_method_id AS delivery_method_key
      , contact_person_id AS contact_person_Key
      , accounts_person_id AS accounts_person_key
      , salesperson_person_id AS salesperson_person_key
      , packed_by_person_id AS packed_by_person_key
    FROM fact_invoice__source
)

, fact_invoice__cast_type AS (
    SELECT
      CAST(invoice_key AS INTEGER) AS invoice_key
      , CAST(is_credit_note_boolean AS BOOLEAN) AS is_credit_note_boolean
      , CAST(credit_note_reason AS STRING) AS credit_note_reason
      , CAST(customer_purchase_order_number AS STRING) AS customer_purchase_order_number
      , CAST(invoice_date AS DATE) AS invoice_date
      , CAST(confirmed_delivery_at AS DATETIME) AS confirmed_delivery_at
      , CAST(confirmed_received_by AS STRING) AS confirmed_received_by

      , SAFE_CAST(returned_delivery_data AS STRING) AS returned_delivery_data

      , CAST(total_dry_items AS INTEGER) AS total_dry_items
      , CAST(total_chiller_items AS INTEGER) AS total_chiller_items
      , CAST(sales_order_key AS INTEGER) AS sales_order_key
      , CAST(customer_key AS INTEGER) AS customer_key
      , CAST(bill_to_customer_key AS INTEGER) AS bill_to_customer_key
      , CAST(delivery_method_key AS INTEGER) AS delivery_method_key
      , CAST(contact_person_Key AS INTEGER) AS contact_person_Key
      , CAST(accounts_person_key AS INTEGER) AS accounts_person_key
      , CAST(salesperson_person_key AS INTEGER) AS salesperson_person_key
      , CAST(packed_by_person_key AS INTEGER) AS packed_by_person_key
    FROM fact_invoice__rename_column
)

, fact_invoice__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_credit_note_boolean IS TRUE THEN 'Credit Note'
          WHEN is_credit_note_boolean IS FALSE THEN 'Not Credit Note'
          WHEN is_credit_note_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_credit_note
    FROM fact_invoice__cast_type
)

, fact_invoice__enrich AS (
    SELECT
      *
      , JSON_VALUE(returned_delivery_data, '$.ReceivedBy') AS received_by
      , JSON_VALUE(returned_delivery_data, '$.ConNote') AS con_note
      , JSON_VALUE(returned_delivery_data, '$.Status') AS status
      , JSON_VALUE(returned_delivery_data, '$.DeliveredWhen') AS delivered_at
    FROM fact_invoice__convert_boolean
)

, fact_invoice__handle_null AS (
    SELECT
      invoice_key
      , COALESCE(is_credit_note, 'Undefined') AS is_credit_note
      , credit_note_reason
      , customer_purchase_order_number
      , invoice_date
      , confirmed_delivery_at
      , confirmed_received_by
      , total_dry_items
      , total_chiller_items
      , received_by
      , con_note
      , status
      , delivered_at
      , COALESCE(sales_order_key, 0) AS sales_order_key
      , COALESCE(customer_key, 0) AS customer_key
      , COALESCE(bill_to_customer_key, 0) AS bill_to_customer_key
      , COALESCE(delivery_method_key, 0) AS delivery_method_key
      , COALESCE(contact_person_Key, 0) AS contact_person_Key
      , COALESCE(accounts_person_key, 0) AS accounts_person_key
      , COALESCE(salesperson_person_key, 0) AS salesperson_person_key
      , COALESCE(packed_by_person_key, 0) AS packed_by_person_key
    FROM fact_invoice__enrich
)

SELECT *
FROM fact_invoice__enrich


/*
SELECT
  invoice_key
  , is_credit_note
  , credit_note_reason
  , customer_purchase_order_number
  , invoice_date
  , confirmed_delivery_at
  , confirmed_received_by
  , total_dry_items
  , total_chiller_items
  , received_by
  , con_note
  , status
  , delivered_at
  , sales_order_key
  , customer_key
  , bill_to_customer_key
  , delivery_method_key
  , contact_person_Key
  , accounts_person_key
  , salesperson_person_key
  , packed_by_person_key
FROM fact_invoice__handle_null
*/