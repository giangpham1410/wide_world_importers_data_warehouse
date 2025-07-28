WITH
  dim_product__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column AS (
    SELECT
      stock_item_id AS product_key
      , stock_item_name AS product_name
      , brand AS brand_name
      , size
      , is_chiller_stock AS is_chiller_stock_boolean
      , unit_price
      , recommended_retail_price
      , lead_time_days
      , quantity_per_outer
      , tax_rate
      , typical_weight_per_unit
      , supplier_id AS supplier_key
      , color_id AS color_key
      , unit_package_id AS unit_package_type_key
      , outer_package_id AS outer_package_type_key
    FROM dim_product__source
)

, dim_product__cast_type AS (
    SELECT
      CAST(product_key AS INTEGER) AS product_key
      , CAST(product_name AS STRING) AS product_name
      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(brand_name AS STRING) AS brand_name
      , CAST(is_chiller_stock_boolean AS BOOLEAN) AS is_chiller_stock_boolean
    FROM dim_product__rename_column
)

, dim_product__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
          WHEN is_chiller_stock_boolean IS FALSE THEN 'Not Chiller Stock'
          WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_chiller_stock
    FROM dim_product__cast_type 
)

-- xử lý NULL ở những cột NULL từ gốc
, dim_product__handle_null AS (
    SELECT
      product_key
      , product_name
      , supplier_key
      , COALESCE(brand_name, 'Undefined') AS brand_name
      , is_chiller_stock
    FROM dim_product__convert_boolean
)

, dim_product__add_undefined_record AS (
    SELECT
      product_key
      , product_name
      , supplier_key
      , brand_name
      , is_chiller_stock
    FROM dim_product__handle_null

    UNION ALL
    SELECT
        0 AS product_key
      , 'Undefined' AS product_name
      , 0 AS supplier_key
      , 'Undefined' AS brand_name
      , 'Undefined' AS is_chiller_stock

    UNION ALL
    SELECT
        -1 AS product_key
      , 'Invalid' AS product_name
      , -1 AS supplier_key
      , 'Invalid' AS brand_name
      , 'Invalid' AS is_chiller_stock
    

)

SELECT *
FROM dim_product__rename_column

/*
SELECT
  dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_product.is_chiller_stock

  , dim_product.supplier_key
  , COALESCE(dim_supplier.supplier_name, 'Invalid') AS supplier_name
FROM dim_product__add_undefined_record dim_product
  LEFT JOIN {{ ref('dim_supplier') }} dim_supplier
    ON dim_product.supplier_key = dim_supplier.supplier_key
*/