{{
  config(
    materialized='table'
  )
}}

select month, region, destination, hotel resort, module, kpi 
from `vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_ECOM_KPI`