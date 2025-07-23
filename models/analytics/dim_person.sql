WITH
  dim_person__source AS (
    SELECT 
      *
    FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS (
    SELECT
      person_id AS person_key
      , full_name AS full_name
    FROM dim_person__source
)

SELECT *
FROM dim_person__rename_column