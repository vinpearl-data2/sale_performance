{{
  config(
    materialized='table'
  )
}}

SELECT DISTINCT * FROM `vp-dwh-prod-c827.MAPPING.REVENUE_SALES_GROUP`