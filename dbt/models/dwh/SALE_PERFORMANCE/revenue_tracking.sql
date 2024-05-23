{{
  config(
    materialized='table'
  )
}}

with tour as(
select distinct date(createddate,'Asia/Ho_Chi_Minh') bookingdate,
                                  ordercode,
                                  SaleChannel,
                                  case when channel =  1 then 'Online Website'
                          when channel =  2 then 'Online App'
                          when channel =  3 then 'Online Chatbot'
                          when channel = 4 then 'Offline Website'
                          when channel = 10 then 'Booking VinWonder'
                          when channel = 11 then 'VinWonder App'
                          end as channel,
                                  membershipcode,
                                  totalamount,
                                  surcharge, --phụ thu,
                                  couponvalue,
                                  totalchangeratecode,
                                  case when groupname is null then upper(skuname) 
                                  else upper(groupname) 
                                  end as groupname,
                                  property hotelname,
                                  ticketcode,
                                  ticketprice,
                                  checkcode,
                                  
from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.REPORT_ORDERREPORTS` 
where paymentstatus = 2 and orderitemtype = 2 and date(createddate,'Asia/Ho_Chi_Minh') >= "2023-01-01"
) 

,property_tour as (
   
   with b1 as (
             select distinct * except(ModifiedDate,CreatedDate),
              Date(ModifiedDate,'Asia/Ho_Chi_Minh') ModifiedDate,
              Date(CreatedDate,'Asia/Ho_Chi_Minh') CreatedDate,
                             max(Date(ModifiedDate,'Asia/Ho_Chi_Minh')) over(partition by Code) max_date,
                             max(Date(CreatedDate,'Asia/Ho_Chi_Minh')) over(partition by Code) max_create
             from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.TOUR_TOURTICKET`
             where true and IsAvailable=1)
, b as (
   select * from b1
   where true and ((max_date is not null and ModifiedDate=max_date) or (max_date is null and CreatedDate=max_create))
)

,c1 as (
   select c.TourTicketId,
          d.name,
          Date(d.CreatedDate,'Asia/Ho_Chi_Minh') CreatedDate,
          Date(d.ModifiedDate,'Asia/Ho_Chi_Minh') ModifiedDate,
         max(Date(d.ModifiedDate,'Asia/Ho_Chi_Minh')) over(partition by c.TourTicketId) max_date
   from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.TOUR_TOURTICKETPROPERTIES` c
   left join `vp-data-lake-prod-c827.VINPEARL_TRAVEL.VOUCHER_PROPERTYI18NS` d on c.PropertyId=d.PropertyId)

, c2 as (
  select 
  TourTicketId,
  name,ModifiedDate,CreatedDate,
  max(CreatedDate) over(partition by TourTicketId) max_create,
  max_date
  from c1 
  where true )
 
 ,c as (
 select 
  TourTicketId,
  name,
  ModifiedDate,CreatedDate,
  max_create,max_date
 from c2
 where 
 CreatedDate=max_create)

 , final as (
  select distinct a.ordercode,
                  a.TicketCode,
                  c.name,
  from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.REPORT_ORDERREPORTS` a 
  left join b on a.TicketCode=b.code
  left join c on b.id=c.TourTicketId
  where true and a.TicketCode is not null)

 select a.* except(hotelname, ticketcode), b.name hotelname from tour a
 left join final b on a.ordercode = b.ordercode and a.ticketcode = b.ticketcode)


,price_tour as (
   with aa as (
    select bookingdate, 
                ordercode, 
                salechannel, 
                channel,
                membershipcode,
                hotelname,  
                surcharge, 
                couponvalue, 
                totalchangeratecode, 
                totalamount,
                groupname,
              
               sum(ticketprice) amount,
           from property_tour 
         group by 1,2,3,4,5,6,7,8,9,10,11)

     ,bb as (
            select ordercode,count(*) counting 
            from aa
            group by 1)

     ,cc as (
            select aa.* , bb.counting 
            from aa
            left join bb on aa.ordercode = bb.ordercode)

     ,final as (
       select * except(surcharge, totalchangeratecode, counting, amount),
                (amount + ifnull(surcharge,0)/counting + ifnull(totalchangeratecode,0)/counting- ifnull(couponvalue,0)/counting) final_amount 
       from cc)

      select * except (totalamount, final_amount), case when final_amount is null then totalamount else final_amount end as total_amount
      from final)

,tour_final as (
select distinct     bookingdate,ordercode, salechannel,channel,membershipcode,total_amount totalamount, couponvalue,
                    case 
                     when groupname like '%WELLNESS%' then "PEARL WELLNESS"
                     when groupname like "%XE ĐÓN TIỄN%" then "XE ĐÓN TIỄN"
                     when groupname like '%EXPRESS COMBO' then 'COMBO PQE'
                     when groupname like '%COMBO 30/4 - 2023%' then 'MAYFEST COMBO'
                     when groupname like '%GOLF AWAY%' then 'GOLF AWAY RETREAT'
                     when groupname like '%XUÂN%'  then 'XUÂN VIVU'
                     when groupname like '%FAMILY%' then groupname
                     when groupname like '%AMPEARL PEARL%' then 'THẺ VIP PEARL'
                     when groupname like '%XUÂN%' then 'XUÂN VIVI'
                     when groupname like '%VINPEARL AQUARIUM%' then 'TIMES CITY - THỦY CUNG'
                     when groupname like '%GRAND WORLD PHÚ QUỐC%' then 'GRAND WORLD PHÚ QUỐC'
                     when groupname like '%KHU VUI CHƠI VINKE%' then 'TIMES CITY - VINKE'
                     when groupname like '%[VINWONDERS + VINPEARL SAFARI PHÚ QUỐC]%' then 'VINWONDERS + VINPEARL SAFARI PHÚ QUỐC'
                     else groupname
                     end as groupname,
                     hotelname,
                     "TOUR & TN" as system,
                     cast(null as string) as Nationality
from price_tour)


, bundle as (
  select     date(createddate,'Asia/Ho_Chi_Minh') bookingdate,
                                  ordercode,
                                  SaleChannel,
                                  case when channel =  1 then 'Online Website'
                          when channel =  2 then 'Online App'
                          when channel =  3 then 'Online Chatbot'
                          when channel = 4 then 'Offline Website'
                          when channel = 10 then 'Booking VinWonder'
                          when channel = 11 then 'VinWonder App'
                          end as channel,
                                  membershipcode,
                                  totalamount,
                                  lower(rateplanname) rateplanname,
                                  hotelname,
                                  couponvalue
  from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.REPORT_ORDERBUNDLEREPORTS` 
  where paymentstatus = 2 and date(createddate,'Asia/Ho_Chi_Minh') >= "2023-01-01"
)
, bundle1 as (select distinct bookingdate, 
                             ordercode, 
                             salechannel, 
                             channel, 
                             membershipcode,
                             totalamount, 
                             couponvalue,
                             case when lower(rateplanname) like '%festive%' then 'FESTIVE'
                                  when lower(rateplanname) like '%cảm hứng bất tận%' then 'CẢM HỨNG BẤT TẬN(BTB)'
                                  when lower(rateplanname) like '%combo tận hưởng%' then 'COMBO TẬN HƯỞNG (BAR-25%)'
                                  when lower(rateplanname) like '%theo mùa%' then 'KHUYẾN MÃI THEO MÙA'
                                  when lower(rateplanname) like '%ưu đãi đặt sớm%' then 'ƯU ĐÃI ĐẶT SỚM'
                                  when lower(rateplanname) like '%free vinwonders%' then 'FREE VINWONDERS' 
                                  when lower(rateplanname) like '%flash sale%' then 'FLASH SALE'
                                  when lower(rateplanname) like '%gia đình tận hưởng%' then 'GÓI GIA ĐÌNH TẬN HƯỞNG'
                                  when lower(rateplanname) like '%gia đình nghỉ dưỡng%' then 'GÓI GIA ĐÌNH NGHỈ DƯỠNG'
                                  when lower(rateplanname) like '%gói family%' then 'GÓI GIA ĐÌNH'
                             end as groupname,
                             hotelname,
                            "BUNDLE" as system,
                             cast(null as string) as Nationality

from bundle
  where rateplanname is not null)


,crs as 
 (select date(booking_date,'Asia/Ho_Chi_Minh') BookingDate,
       date(Arrival_Date) ArrivalDate,
       date(Departure_date) DepartureDate,
       status Status,
       Property_Code PropertyCode,
       Property_Name PropertyName,
       Itinerary_Number ItineraryNumber,
       Room_Confirmation_Number RoomConfirmationNumber,
       Cancellation_Number CancellationNumber,
       Distribution_Channel DistributionChannel,
       Room_Type_Codes RoomTypeCodes,
       Room_Type_Names RoomTypeNames,
       Rate_Plan_Codes RatePlanCodes,
       Rate_Plan_Names RatePlanNames,
       Source_Code SourceCode,
       Market_Code MarketCode,
       Number_Of_Adults NumberOdAdults,
       Number_Of_Other_Occupancies NumberOfOtherOccupancies,
       MainGuest MainGuest,
       GuestEmail,
       GuestPhone,
       Address,
       Nationality detected,
       TravelAgent,
       IncludePackage,
       BookablePackage,
       BookablePackageAmount,
       Booker,
       BookerEmail,
       BookerPhone,
       date_diff(date(Departure_Date), date(Arrival_Date), day) RoomNights,
       Notes,
       Total,
       CancelPenaltyAmount CancelPenaltyAmount,
       CancelPenaltyPercent CancelPenaltyPercent,
       code.groupcode 

from `vp-dwh-prod-c827.CIRRUS.res_booking__reservation`  a
left join vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_RATE_PLAN_CODES code on a.Rate_Plan_Codes = code.RatePlanCodes
where organization not like '%melia%' and date(booking_date,'Asia/Ho_Chi_Minh') >= "2023-01-01" )


,hotelreport as (
select distinct date(createddate,'Asia/Ho_Chi_Minh') bookingdate, 
                ordercode,
                salechannel,
          case  when channel =  1 then 'Online Website'
                when channel =  2 then 'Online App'
                when channel =  3 then 'Online Chatbot'
                when channel = 4 then 'Offline Website'
                when channel = 10 then 'Booking VinWonder'
                when channel = 11 then 'VinWonder App'
                end as channel,
                membershipcode,
                totalamount,
                couponvalue,
                rateplancode,
                hotelname,
                nationality,
                case when rateplancodes in ('PR12108FB',  'PR12109FB', 'PR12108BB', 'PR12109FBB') then "FREE VINWONDERS" 
                                                   else groupcode
                                                   end as groupname,

from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.REPORT_ORDERHOTELREPORTS` hotelreport
left join `vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_RATE_PLAN_CODES` code on hotelreport.rateplancode = code.RatePlanCodes
where date(createddate) >= "2023-01-01" and paymentstatus = 2)

,hotel as (
select distinct bookingdate,
                ordercode,
                salechannel,
                channel,
                membershipcode,
                totalamount,
                couponvalue,
                groupname,
                hotelname,
                "HOTEL" as system,
                nationality,

from hotelreport
left join `vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_RATE_PLAN_CODES` code on hotelreport.rateplancode = code.RatePlanCodes
Where groupname is null or groupname not in ("TABS", 'CBNV', 'Vinfast', 'VINFAST', 'DNMP')
)


,vinpearl as
 (select                          bookingdate,
                                  roomconfirmationnumber ordercode,
                                  distributionchannel as salechannel,
                                  "0" as channel,
                                  cast(null as string) membershipcode,
                                  cast(total as numeric) total,
                                  0 as couponvalue,
                                  groupcode groupname,
                                  propertyname hotelname,
                                  "VINPEARL" as system,
                                  detected
  from   crs

  where status in ('Checkout', 'Inhouse', 'Reserved', 'Changed') and groupcode in ('TABS', 'Vinfast', 'VINFAST', 'DNMP', 'CBNV')  )

,final as (
select * from tour_final 
union all
select * from bundle1
union all
select * from hotel
union all
select * from vinpearl)

,final1 as (

 with a as (
   select distinct SPLIT(a.SYSTEM_ID, r' ## ')[offset(1)] SYSTEM_ID,
                   b.VALID_NATIONALITY NATIONALITY
   from `vp-dwh-prod-c827.DATA_GOVERNANCE.DG_VPT` a
   left join `vp-dwh-prod-c827.DATA_GOVERNANCE.C360_RAW_ALL_CLEANED` b on a.SYSTEM_ID=b.SYSTEM_ID
   where true 
   and a.SYSTEM_ID like '%GUEST%'
   and b.VALID_NATIONALITY is not null)

 ,b as (
  select distinct
   ordercode,
   id,
   LanguageCode
  from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.ORDER_ORDERS` 
  where true)

  select final.* except(nationality), 
    case when final.nationality is null then a.nationality
        else final.nationality
    end as nationality
  from final
  left join b on final.ordercode=b.ordercode
  left join a on a.SYSTEM_ID=b.id
  where true)

,final2 as (
select distinct final1.* except(hotelname),
         case when short_nation = "01. VN" then "Vietnam"
              when short_nation = "04. Hàn" then "Korea"
              when short_nation in ("08. Úc&Nz" , "09. Mỹ&Canada") then "Úc/Mỹ/Canada/Nz"
              else "Others"
          end as short_nation,
          case 
            when hotelname is null then "Hệ thống Vinpearl" 
            when ordercode in ("VPT-HXAKD170523","VPT-ENPAK160623","VPT-UXDHE160623","VPT-MDDPC130623", "VPT-ZYAHN210623","VPT-SKGXQ210623","VPT-GQGPG260923") then "Hệ thống Vinpearl" --case adhoc sales fix, cho nhiều cơ sở nhưng bị vào riêng Hạ Long
            when hotelname ="빈홀리데이즈 피에스타 푸꾸옥" then "VinHolidays Fiesta Phú Quốc"
            when hotelname ="빈펄 리조트&골프 남호이안" then "Vinpearl Resort & Golf Nam Hội An"
            when hotelname ="빈펄 리조트 & 스파 나트랑 베이" then "Vinpearl Resort & Spa Nha Trang Bay"
            else hotelname 
          end as hotelname,
          nat_code.* except(short_nation, nationality)

from final1
left join `vp-dwh-prod-c827.MAPPING.NATION_CODE` nat_code on final1.nationality = nat_code.nation_code)

select distinct * 
  replace (
    case  when regexp_contains(hotelname, r'Hòn Tằm') then 'Hòn Tằm Resort Nha Trang'
          when regexp_contains(hotelname, r'Stay And Fun Ocean Park|StayNFun Ocean Park') then 'StayNFun Ocean Park 2' 
          when regexp_contains(hotelname, r'VinHolidays') then 'VinHolidays Fiesta Phú Quốc'
          when regexp_contains(hotelname, r'Beachfront Nha Trang') then 'Vinpearl Beachfront Nha Trang' 
          when regexp_contains(hotelname, r'Luxury Nha Trang') then 'Vinpearl Luxury Nha Trang'
          when regexp_contains(hotelname, r'Resort & Golf Nam Hoi An|Resort & Golf Nam Hội An|Resort và Golf Nam Hội An|Resort and Golf Nam Hoi An|Resort & Spa Hội An') then 'Vinpearl Resort & Golf Nam Hội An' 
          when regexp_contains(hotelname, r'Resort và Spa Hạ Long|Resort & Spa Hạ Long|Resort and Spa Ha Long|Resort & Spa Ha Long') then 'Vinpearl Resort & Spa Hạ Long' 
          when regexp_contains(hotelname, r'Nha Trang Bay') then 'Vinpearl Resort & Spa Nha Trang Bay' 
          when regexp_contains(hotelname, r'Resort và Spa Phú Quốc|Resort & Spa Phú Quốc|Resort & Spa Phu Quoc|Resort and Spa Phú Quốc') then 'Vinpearl Resort & Spa Phú Quốc' 
          when regexp_contains(hotelname, r'Resort Nha Trang|Nha Trang Resort') then 'Vinpearl Resort Nha Trang' 
          when regexp_contains(hotelname, r'Wonderworld') then 'Vinpearl Wonderworld Phú Quốc' 
          else hotelname
    end as hotelname
  )
from final2