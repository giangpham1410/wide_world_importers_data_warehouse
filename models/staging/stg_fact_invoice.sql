WITH
  fact_invoice__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__invoices`
)

, fact_invoice__rename_column AS (
    SELECT
      invoice_id AS invoice_key
      , is_credit_note
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

SELECT *
FROM fact_invoice__rename_column