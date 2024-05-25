{{
  config(
    materialized='table'
  )
}}

-- dim property
with property as (
  select 'Hòn Tằm Resort Nha Trang' as propertyname
  union all
  select 'Vinpearl Luxury Nha Trang' as propertyname
  union all
  select 'Vinpearl Resort & Spa Nha Trang Bay' as propertyname
  union all
  select 'Vinpearl Resort Nha Trang' as propertyname
  union all
  select 'VinHolidays Fiesta Phú Quốc' as propertyname
  union all
  select 'Vinpearl Beachfront Nha Trang' as propertyname
  union all
  select 'Vinpearl Resort & Spa Phú Quốc' as propertyname
  union all
  select 'Vinpearl Resort & Spa Hạ Long' as propertyname
  union all
  select 'Vinpearl Resort & Golf Nam Hội An' as propertyname
  union all
  select 'Vinpearl Wonderworld Phú Quốc' as propertyname
  union all
  select 'StayNFun Ocean Park 2' as propertyname
)

select 
  propertyname,
  case when propertyname like '%Nha Trang%' then 'Nha Trang'
       when propertyname like '%Hội An%' then 'Hội An'
       when propertyname like '%Phú Quốc%' then 'Phú Quốc'
       when propertyname like '%Hạ Long%' then 'Hạ Long'
       when propertyname like '%Hòn Tằm%' then 'Nha Trang'
       when propertyname = 'StayNFun Ocean Park 2' then 'Hà Nội'
  end as destination,

  case when regexp_contains(propertyname, r'Nha Trang|Đà Nẵng|Hội An|Hòn Tằm') then 'Region 1'
       when regexp_contains(propertyname, r'Hạ Long|Phú Quốc') then 'Region 2'
       when regexp_contains(propertyname, r'Stay And Fun|StayNFun') then 'Region 3' -- cái này chưa có confirm
  end as Region
  
from property