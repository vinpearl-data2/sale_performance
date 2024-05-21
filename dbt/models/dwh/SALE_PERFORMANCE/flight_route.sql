{{
  config(
    materialized='table'
  )
}}

with a as (
  select 
       date(createddate) bookingdate,
       ordercode,
       case when channel =  1 then 'Online Website'
            when channel =  2 then 'Online App'
            when channel =  3 then 'Online Chatbot'
            when channel = 4 then 'Offline Website'
            when channel = 10 then 'Booking VinWonder'
            when channel = 11 then 'VinWonder App'
       end as channel,
       salechannel,
       membershipcode,
       totalamount,
       case when lower(startpoint) like '%nội bài%' or lower(startpoint) like '%noi bai%' then 'HN'
            when lower(startpoint) like '%tan son nhat%' or lower(startpoint) like '%tân sơn nhất%' then 'HCM'
            when lower(startpoint) like '%cam ranh%' then 'NT'
            when lower(startpoint) like '%cat bi%' or lower(startpoint)like '%cát bi%' then 'HP'
            when lower(startpoint) like '%phú quốc%' or lower(startpoint) like '%phu quoc%' then 'PQ'
            when lower(startpoint) like '%đà nẵng%' or lower(startpoint) like '%da nang%' or lower(startpoint) like '%danang%' then 'DN'
            when regexp_contains(lower(startpoint),r'trà nóc|tra noc|can tho') then 'CT'
            when lower(startpoint) like '%incheon%' or lower(startpoint) like '%gimhae%' then 'HQ'
            when lower(startpoint) like '%suvarnabhumi%' then 'TL'
            when lower(startpoint) like '%vân đồn%' or lower(startpoint) like '%van don%' then 'QN'
       end as startpoint, 

       case when lower(endpoint) like '%nội bài%' or lower(endpoint) like '%noi bai%' then 'HN'
            when lower(endpoint) like '%tan son nhat%' or lower(endpoint) like '%tân sơn nhất%' then 'HCM'
            when lower(endpoint) like '%cam ranh%' then 'NT'
            when lower(endpoint) like '%cat bi%' or lower(endpoint)like '%cát bi%' then 'HP'
            when lower(endpoint) like '%phú quốc%' or lower(endpoint) like '%phu quoc%' then 'PQ'
            when lower(endpoint) like '%đà nẵng%' or lower(endpoint) like '%da nang%' or lower(endpoint) like '%danang%' then 'DN'
            when regexp_contains(lower(endpoint),r'trà nóc|tra noc|can tho') then 'CT'
            when lower(endpoint) like '%incheon%'or lower(endpoint) like '%gimhae%' then 'HQ'
            when lower(endpoint) like '%suvarnabhumi%' then 'TL'
            when lower(endpoint) like '%vân đồn%' or lower(endpoint) like '%van don%' then 'QN'
       end as endpoint

  from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.REPORT_ORDERBUNDLEREPORTS` 
  where  true and paymentstatus = 2 and date(createddate) >= "2023-01-01" and OrderItemType = 3
)


,final as (
  select 
    bookingdate, 
    ordercode, 
    channel, 
    salechannel, 
    membershipcode, 
    totalamount, 
    case when string_agg(startpoint, "-")  = 'HCM-PQ' or string_agg(startpoint, "-") = 'PQ-HCM' then 'HCM-PQ'
        when string_agg(startpoint, "-")  = 'HN-PQ' or string_agg(startpoint, "-") = 'PQ-HN' then 'HN-PQ'
        when string_agg(startpoint, "-")  = 'CT-PQ' or string_agg(startpoint, "-") = 'PQ-CT' then 'CT-PQ'
        when string_agg(startpoint, "-")  = 'DN-PQ' or string_agg(startpoint, "-") = 'PQ-DN' then 'DN-PQ'
        when string_agg(startpoint, "-")  = 'HP-PQ' or string_agg(startpoint, "-") = 'PQ-HP' then 'HP-PQ'
        when string_agg(startpoint, "-")  = 'NT-PQ' or string_agg(startpoint, "-") = 'PQ-NT' then 'NT-PQ'
        when string_agg(startpoint, "-")  = 'HQ-PQ' or string_agg(startpoint, "-") = 'PQ-HQ' then 'HQ-PQ'
        when string_agg(startpoint, "-")  = 'HCM-DN' or string_agg(startpoint, "-") = 'DN-HCM' then 'HCM-DN'
        when string_agg(startpoint, "-")  = 'HN-DN' or string_agg(startpoint, "-") = 'DN-HN' then 'HN-DN'
        when string_agg(startpoint, "-")  = 'NT-DN' or string_agg(startpoint, "-") = 'DN-NT' then 'DN-NT'
        when string_agg(startpoint, "-")  = 'CT-DN' or string_agg(startpoint, "-") = 'DN-CT' then 'CT-DN'
        when string_agg(startpoint, "-")  = 'HCM-NT' or string_agg(startpoint, "-") = 'NT-HCM' then 'HCM-NT'
        when string_agg(startpoint, "-")  = 'HN-NT' or string_agg(startpoint, "-") = 'NT-HN' then 'HN-NT'
        when string_agg(startpoint, "-")  = 'HP-NT' or string_agg(startpoint, "-") = 'NT-HP' then 'HP-NT'
        when string_agg(startpoint, "-")  = 'HQ-NT' or string_agg(startpoint, "-") = 'NT-HQ' then 'HQ-NT'
        when string_agg(startpoint, "-")  = 'QN-HCM' or string_agg(startpoint, "-") = 'HCM-QN' then 'HCM-QN'
    end as route, 
    string_agg(endpoint, "-") return_route
  from a
  group by 1,2,3,4,5,6
)

select * ,
  case when route in ('HCM-PQ', 'HN-PQ', 'CT-PQ', 'DN-PQ', 'HP-PQ', 'NT-PQ', 'HQ-PQ') then 'Phu Quoc Region'
      when route in ('HCM-DN', 'HN-DN', 'NT-DN', 'CT-DN') then 'Da Nang - Hoi An Region'
      when route in ('HCM-NT', 'HN-NT', 'HP-NT', 'HQ-NT') then 'Nha Trang Region'
      when route in ('HCM-QN') then 'Ha Long Region'
  end as destination
from final