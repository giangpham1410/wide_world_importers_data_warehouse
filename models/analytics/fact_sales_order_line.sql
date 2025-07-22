SELECT 
  order_line_id AS sales_order_line_key
  , unit_price
  , quantity
  , stock_item_id AS product_key
  , unit_price * quantity AS gross_amount
FROM `vit-lam-data.wide_world_importers.sales__order_lines`
