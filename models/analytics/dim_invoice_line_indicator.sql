WITH
  dim_is_credit_note AS (
    SELECT
      TRUE AS is_credit_note_key
      , 'Credit Note' AS is_credit_note
    
    UNION ALL
    SELECT
      FALSE AS is_credit_note_key
      , 'Not Credit Note' AS is_credit_note
)

SELECT
  FARM_FINGERPRINT(
    CONCAT(
      is_credit_note_key
      , ','
      , package_type_key
    )
  ) AS invoice_line_indicator_key

  , dim_is_credit_note.is_credit_note_key
  , dim_is_credit_note.is_credit_note

  , dim_package_type.package_type_key
  , dim_package_type.package_type_name
FROM dim_is_credit_note AS dim_is_credit_note
  CROSS JOIN {{ ref('dim_package_type') }} dim_package_type