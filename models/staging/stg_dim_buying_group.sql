WITH
  dim_buying_group__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, dim_buying_group__rename_column AS (
    SELECT
      buying_group_id AS buying_group_key
      , buying_group_name
    FROM dim_buying_group__source
)


SELECT *
FROM dim_buying_group__rename_column