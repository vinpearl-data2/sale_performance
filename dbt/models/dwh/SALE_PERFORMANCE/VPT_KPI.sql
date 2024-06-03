{{
  config(
    materialized='table'
  )
}}

select month, region, destination, hotel resort, module, kpi 
from `vp-dwh-prod-c827.VIN3S_DATATMART_VINPEARL.ECOM_KPI`