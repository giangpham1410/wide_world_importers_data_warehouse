WITH
  dim_is_undersupply_backordered AS (
    SELECT
      1 AS is_undersupply_backordered_key
      , 'Undersupply Backordered' AS is_undersupply_backordered
    UNION ALL
    SELECT
      2 AS is_undersupply_backordered_key
      , 'Not Undersupply Backordered' AS is_undersupply_backordered
)
SELECT *
FROM dim_is_undersupply_backordered
  CROSS JOIN {{ ref('dim_package_type') }} AS dim_package_type
ORDER BY 1,3