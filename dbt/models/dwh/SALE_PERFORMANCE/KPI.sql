{{
  config(
    materialized='table'
  )
}}

with kpi as (
  select * replace (
    case when regexp_contains(PropertyCode, r'Hòn Tằm') then 'Hòn Tằm Resort Nha Trang'
        when regexp_contains(PropertyCode, r'Stay And Fun Ocean Park|StayNFun Ocean Park') then 'StayNFun Ocean Park 2' 
        when regexp_contains(PropertyCode, r'VinHolidays') then 'VinHolidays Fiesta Phú Quốc'
        when regexp_contains(PropertyCode, r'Beachfront Nha Trang') then 'Vinpearl Beachfront Nha Trang' 
        when regexp_contains(PropertyCode, r'Luxury Nha Trang') then 'Vinpearl Luxury Nha Trang'
        when regexp_contains(PropertyCode, r'Resort & Golf Nam Hoi An|Resort & Golf Nam Hội An|Resort và Golf Nam Hội An|Resort and Golf Nam Hoi An') then 'Vinpearl Resort & Golf Nam Hội An' 
        when regexp_contains(PropertyCode, r'Resort và Spa Hạ Long|Resort & Spa Hạ Long|Resort and Spa Ha Long') then 'Vinpearl Resort & Spa Hạ Long' 
        when regexp_contains(PropertyCode, r'Nha Trang Bay') then 'Vinpearl Resort & Spa Nha Trang Bay' 
        when regexp_contains(PropertyCode, r'Resort và Spa Phú Quốc|Resort & Spa Phú Quốc|Resort & Spa Phu Quoc') then 'Vinpearl Resort & Spa Phú Quốc' 
        when regexp_contains(PropertyCode, r'Resort Nha Trang|Nha Trang Resort') then 'Vinpearl Resort Nha Trang' 
        when regexp_contains(PropertyCode, r'Wonderworld') then 'Vinpearl Wonderworld Phú Quốc' 
        else null
    end as PropertyCode
  ),
  case when regexp_contains(PropertyCode, r'Nha Trang|Đà Nẵng|Hội An|Hòn Tằm') then 'Region 1'
         when regexp_contains(PropertyCode, r'Hạ Long|Phú Quốc') then 'Region 2'
         when regexp_contains(PropertyCode, r'Stay And Fun|StayNFun') then 'Region 3' -- cái này chưa có confirm
    end as Custom

  from `vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_MONTHLY_KPI`
)

select *
from kpi
where PropertyCode is not null