WITH
  dim_is_undersupply_backordered AS (
    SELECT
      TRUE AS is_undersupply_backordered_key
      , 'Undersupply Backordered' AS is_undersupply_backordered

    UNION ALL
    SELECT
      FALSE AS is_undersupply_backordered_key
      , 'Not Undersupply Backordered' AS is_undersupply_backordered
)
SELECT
  CONCAT(
    dim_is_undersupply_backordered.is_undersupply_backordered_key
    , ','
    , dim_package_type.package_type_key)
    AS sales_order_line_indicator_key
  , dim_is_undersupply_backordered.is_undersupply_backordered_key
  , dim_is_undersupply_backordered.is_undersupply_backordered
  , dim_package_type.package_type_key
  , dim_package_type.package_type_name
FROM dim_is_undersupply_backordered AS dim_is_undersupply_backordered
  CROSS JOIN {{ ref('dim_package_type') }} AS dim_package_type
ORDER BY 1,3