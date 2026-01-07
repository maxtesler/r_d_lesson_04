{{
  config(
    materialized='table'
  )
}}

SELECT 
  id,
  name,
  value,
  created_at,
  value * 2 as doubled_value
FROM 
  raw.example_data