{{
  config(
    materialized='table'
  )
}}

-- dim property
select distinct 
  case when `name` = 'Vinpearl Sealink Nha Trang' then 'Vinpearl Discovery Sealink Nha Trang'
       when `name` = 'Vinpearl Golflink Nha Trang' then 'Vinpearl Discovery Golflink Nha Trang'
       when `name` = 'Vinpearl Wonderworld Phú Quốc' then 'Vinpearl Discovery Wonderworld Phú Quốc'
       when `name` = 'Vinpearl Beachfront Nha Trang' then 'Vinpearl Condotel Beachfront Nha Trang'
       when regexp_contains(`name`, r'Stay And Fun Ocean Park|StayNFun Ocean Park') then 'Stay And Fun Ocean Park'
       else `name`
  end as propertyname,

  case when `name` like '%Nha Trang%' then 'Nha Trang'
       when `name` like '%Đà Nẵng%' then 'Đà Nẵng'
       when `name` like '%Hội An%' then 'Hội An'
       when `name` like '%Phú Quốc%' then 'Phú Quốc'
       when `name` like '%Hạ Long%' then 'Hạ Long'
       when `name` like '%Hòn Tằm%' then 'Nha Trang'
       when regexp_contains(`name`, r'Stay And Fun Ocean Park|StayNFun Ocean Park') then 'Hà Nội'
  end as destination,

  case when regexp_contains(`name`, r'Nha Trang|Đà Nẵng|Hội An|Hòn Tằm') then 'Region 1'
       when regexp_contains(`name`, r'Hạ Long|Phú Quốc') then 'Region 2'
       when regexp_contains(`name`, r'Stay And Fun|StayNFun') then 'Region 3' -- cái này chưa có confirm
  end as Region
  
from `vp-dwh-prod-c827.CIHMS.prod_pms_property_pro_hotel` 
where name in ('Vinpearl Resort & Spa Đà Nẵng',
              'Vinpearl Resort & Spa Hội An',
              'VinHolidays Fiesta Phú Quốc',
              'Vinpearl Luxury Nha Trang',
              'VinOasis Phú Quốc',
              'Vinpearl Beachfront Nha Trang',
              'Vinpearl Wonderworld Phú Quốc',
              'Vinpearl Sealink Nha Trang',
              'Vinpearl Golflink Nha Trang',
              'Vinpearl Resort & Spa Hạ Long',
              'Vinpearl Resort Nha Trang',
              'Vinpearl Resort & Spa Nha Trang Bay',
              'Vinpearl Resort & Golf Nam Hội An',
              'Vinpearl Resort & Spa Phú Quốc',
              'Hòn Tằm Resort'
              )
or regexp_contains(`name`, r'Stay And Fun|StayNFun')