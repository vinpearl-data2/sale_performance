{{
  config(
    materialized='table'
  )
}}

with kpi as (
  select * replace (
    case when regexp_contains(propertyname, r'Hòn Tằm') then 'Hòn Tằm Resort Nha Trang'
        when regexp_contains(propertyname, r'Stay And Fun Ocean Park|StayNFun Ocean Park') then 'StayNFun Ocean Park 2' 
        when regexp_contains(propertyname, r'VinHolidays') then 'VinHolidays Fiesta Phú Quốc'
        when regexp_contains(propertyname, r'Beachfront Nha Trang') then 'Vinpearl Beachfront Nha Trang' 
        when regexp_contains(propertyname, r'Luxury Nha Trang') then 'Vinpearl Luxury Nha Trang'
        when regexp_contains(propertyname, r'Resort & Golf Nam Hoi An|Resort & Golf Nam Hội An|Resort và Golf Nam Hội An|Resort and Golf Nam Hoi An') then 'Vinpearl Resort & Golf Nam Hội An' 
        when regexp_contains(propertyname, r'Resort và Spa Hạ Long|Resort & Spa Hạ Long|Resort and Spa Ha Long') then 'Vinpearl Resort & Spa Hạ Long' 
        when regexp_contains(propertyname, r'Nha Trang Bay') then 'Vinpearl Resort & Spa Nha Trang Bay' 
        when regexp_contains(propertyname, r'Resort và Spa Phú Quốc|Resort & Spa Phú Quốc|Resort & Spa Phu Quoc') then 'Vinpearl Resort & Spa Phú Quốc' 
        when regexp_contains(propertyname, r'Resort Nha Trang|Nha Trang Resort') then 'Vinpearl Resort Nha Trang' 
        when regexp_contains(propertyname, r'Wonderworld') then 'Vinpearl Wonderworld Phú Quốc' 
        else null
    end as PropertyName
  )
  from `vp-dwh-prod-c827.VIN3S_DATATMART_VINPEARL.RATE_PLAN_CODES`
)

select *
from kpi 
where PropertyName is not null