SELECT
  person_key AS contact_person_key
  , full_name AS contact_full_name
  , preferred_name AS contact_preferred_name
FROM {{ ref('dim_person') }}
WHERE
  is_external_logon_provider = 'Not External Logon Provider'
  OR person_key IN (0, -1)