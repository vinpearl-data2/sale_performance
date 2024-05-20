{{
  config(
    materialized='table'
  )
}}

select *, case when propertycode like '%Nha Trang%' then "Region 1" 
               when propertycode like '%Đà Nẵng%' or propertycode like '%Hội An%' or propertycode like '%Hạ Long%' then 'Region 2'
               else "Region 3"
               end as Custom


from `vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_MONTHLY_KPI`