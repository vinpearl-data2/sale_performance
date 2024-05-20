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
                                  checkcode

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
   with aa as (select bookingdate, 
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
            select aa.* , counting 
            from aa
            left join bb on aa.ordercode = bb.ordercode)

     ,final as (
       select * except(surcharge, couponvalue,totalchangeratecode, counting, amount), 
                (amount + ifnull(surcharge,0)/counting + ifnull(totalchangeratecode,0)/counting- ifnull(couponvalue,0)/counting) final_amount 
       from cc)

      select * except (totalamount, final_amount), case when final_amount is null then totalamount else final_amount end as total_amount
      from final)

--select * from price_tour

,tour_final as (
select distinct     bookingdate,ordercode, salechannel,channel,membershipcode,total_amount totalamount, 
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
                     "TOUR & TN" as system
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
                                  hotelname
  from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.REPORT_ORDERBUNDLEREPORTS`
  where paymentstatus = 2 and date(createddate,'Asia/Ho_Chi_Minh') >= "2023-01-01"
)
, bundle1 as (select distinct bookingdate, 
                             ordercode, 
                             salechannel, 
                             channel, 
                             membershipcode,
                             totalamount, 
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
                            "BUNDLE" as system
from bundle
  where rateplanname is not null)

, crs_ref as(
SELECT distinct
  id,
  Room_Confirmation_Number,
  reservation_reference_id,
  case 
    when (reservation_reference_id not like '%,%' and reservation_reference_id not like ' / %') then REGEXP_EXTRACT(reservation_reference_id,r'[^/]*')
    when reservation_reference_id not like ' / %' then REGEXP_EXTRACT(reservation_reference_id, r",\s*(.*?)\/")
  end as order_code,
  case 
    when reservation_reference_id like ' / %' then REGEXP_EXTRACT(reservation_reference_id,r'/\s*(,?.*)')
  end as crs_code,

FROM `vp-dwh-prod-c827.CIHMS.res_booking__reservation` br
)
, crs_ref_vpt as(
select
  cr.id,
  cr.Room_Confirmation_Number,
  cr.reservation_reference_id,
  coalesce(cr.order_code,o.OrderCode) order_code
from crs_ref cr
left join `vp-dwh-prod-c827.VINPEARL_TRAVEL.HOTEL_RESERVATION` hr on hr.Opera_Confirmation_Number = cr.crs_code
left join `vp-data-lake-prod-c827.VINPEARL_TRAVEL.ORDER_ORDERITEMS` oi on oi.ProductVariantId = hr.ID
left join `vp-data-lake-prod-c827.VINPEARL_TRAVEL.ORDER_ORDERS` o on o.ID = oi.OrderId
where true
and coalesce(cr.order_code,hr.Opera_Confirmation_Number) like 'VPT-%'
)

,crs as 
 (select date(r.booking_date,'Asia/Ho_Chi_Minh') BookingDate,
       date(r.Arrival_Date) ArrivalDate,
       date(r.Departure_date) DepartureDate,
       r.status Status,
       r.Property_Code PropertyCode,
       r.Property_Name PropertyName,
       r.Itinerary_Number ItineraryNumber,
       r.Room_Confirmation_Number RoomConfirmationNumber,
       r.Cancellation_Number CancellationNumber,
       r.Distribution_Channel DistributionChannel,
       r.Room_Type_Codes RoomTypeCodes,
       r.Room_Type_Names RoomTypeNames,
       r.Rate_Plan_Codes RatePlanCodes,
       r.Rate_Plan_Names RatePlanNames,
       r.Source_Code SourceCode,
       r.Market_Code MarketCode,
       r.Number_Of_Adults NumberOdAdults,
       r.Number_Of_Other_Occupancies NumberOfOtherOccupancies,
       r.MainGuest MainGuest,
       r.GuestEmail,
       r.GuestPhone,
       r.Address,
       r.Nationality detected,
       r.TravelAgent,
       r.IncludePackage,
       r.BookablePackage,
       r.BookablePackageAmount,
       r.Booker,
       r.BookerEmail,
       r.BookerPhone,
       date_diff(date(r.Departure_Date), date(r.Arrival_Date), day) RoomNights,
       r.Notes,
       r.Total,
       r.CancelPenaltyAmount CancelPenaltyAmount,
       r.CancelPenaltyPercent CancelPenaltyPercent,
       i.order_code

from `vp-dwh-prod-c827.CIRRUS.res_booking__reservation`  r
left join crs_ref_vpt i on i.id =r.id
where r.organization not like '%melia%')


,crs1 as (
select * except( propertyname),
         case when distributionchannel in ("AGODA", 
        "BCOM", 'BOOKING.COM',"CTRIP",'CTRIP_B2C',"EXPEDIA", "HOTELSCOMBINED","KLOOK_B2C",'KLOOK B2C', "PRESTIGIA","TIKET.COM",'TIKET_COM', "TRAVELOKA","INTERPARK","TIDESQUARE",'TIDESQUARE - OTA', 'DIDA TRAVEL', 'HBD', 'MAKEMYTRIP B2C') then "OTAs"
              --when sourcecode = "DR" then 'DIRECT'
              when distributionchannel = "WEBSITE" then 'WEBSITE/APP'
              when distributionchannel in ('RESERVATION', 'FRONT OFFICE') then 'DIRECT'
              when distributionchannel in ('VINGROUP P&L', 'VINPEARL JSC') or distributionchannel like '%KHACH LE TAP DOAN%' then 'VINGROUP'
              when distributionchannel = "ECOM - B2B" then 'WEBSITE/APP'
              when distributionchannel = "OWNER" then 'CBT'
              when distributionchannel = "VINPEARL – B2B" then 'WEBSITE/APP'
              when distributionchannel = "VPT_BUNDLE" then 'WEBSITE/APP'
              else 'TA'
              end as Channels,
            
         case when propertyname like "%Phú Quốc%" then 'Phú Quốc'
              when propertyname like '%Nha Trang%' then 'Nha Trang'
              when propertyname like '%Nam Hội An%' or propertyname like '%Hội An%' then 'Hội An'
              when propertyname like '%Đà Nẵng%' then 'Đà Nẵng'
              when propertyname like '%Thanh Hóa%' then 'Thanh Hóa'
              when propertyname like '%Hải Phòng%' or propertyname like '%Hai Phong%' then 'Hải Phòng'
              when propertyname like '%Hạ Long%' then 'Hạ Long'
              when propertyname like '%Phủ Lí%' or propertyname like '%Phủ Lý%' then 'Phủ Lý'
              when propertyname like '%Lạng Sơn%' then 'Lạng Sơn'
              when propertyname like '%Cửa Hội%' or propertyname like '%Cửa Lò%' then 'Nghệ An'
              when propertyname like '%Hà Tĩnh%' then 'Hà Tĩnh'
              when propertyname like '%Cần Thơ%' then 'Cần Thơ'
              when propertyname like '%Tây Ninh%' then 'Tây Ninh'
              when propertyname like '%Huế%' then 'Huế'
              when propertyname like '%Landmark%' then 'Tp HCM'
              when propertyname like '%Quảng Bình%' then 'Quảng Bình'
              end as destination,

    `vp-dwh-prod-c827.UP_CROSS_SELL.RoomPackage`(rateplancodes) roompackage,
    case  when propertyname = 'VinOasis Phu Quoc' then 'VinOasis Phú Quốc'
          when propertyname = '빈펄 럭셔리 랜드마크 81' then 'Vinpearl Luxury Landmark 81'
          when propertyname in ('Vinpearl Resort và Golf Nam Hội An', 'Vinpearl Resort & Golf Nam Hoi An', 'Vinpearl Resort and Golf Nam Hoi An') then 'Vinpearl Resort & Golf Nam Hội An'
          when propertyname in ('Vinpearl Resort & Spa Hoi An', "Vinpearl Resort và Spa Hội An") then "Vinpearl Resort & Spa Hội An"
          when propertyname = "Vinpearl Resort & Golf Phu Quoc" or propertyname = 'Vinpearl Resort và Golf Phú Quốc' or propertyname = 'Vinpearl Resort and Golf Phu Quoc' then "Vinpearl Resort & Golf Phú Quốc"
          when propertyname = "Vinpearl Resort và Spa Đà Nẵng" then "Vinpearl Resort & Spa Đà Nẵng"
          when propertyname = "Vinpearl Resort và Spa Hạ Long" or propertyname = 'Vinpearl Resort and Spa Ha Long' then "Vinpearl Resort & Spa Hạ Long"
          when propertyname = "Vinpearl Resort và Spa Long Beach Nha Trang" then "Vinpearl Resort & Spa Long Beach Nha Trang"
          when propertyname = "Vinpearl Resort và Spa Nha Trang Bay" then "Vinpearl Resort & Spa Nha Trang Bay"
          when propertyname in  ("Vinpearl Resort và Spa Phú Quốc", "Vinpearl Resort & Spa Phu Quoc") then "Vinpearl Resort & Spa Phú Quốc"
          when propertyname = 'Vinpearl Hotel Quang Binh' then 'Vinpearl Hotel Quảng Bình'
          when propertyname = 'VinHoliday 1 Phú Quốc' then 'VinHolidays 1 Phú Quốc'
          when propertyname = 'Vinpearl Hotel Rivera Hai Phong' then 'Vinpearl Hotel Rivera Hải Phòng'
          when propertyname = 'Vinpearl Hotel Imperia Hai Phong' then 'Vinpearl Hotel Imperia Hải Phòng'
          when propertyname = 'Vinpearl Discovery 1 Phu Quoc' then 'Vinpearl Discovery 1 Phú Quốc'
          when propertyname = 'Vinpearl Discovery 2 Phu Quoc' then 'Vinpearl Discovery 2 Phú Quốc'
          when propertyname = 'Vinpearl Discovery 3 Phu Quoc' then 'Vinpearl Discovery 3 Phú Quốc'
          when propertyname = 'Vinpearl Hotel Can Tho' or propertyname = 'Vinpearl Cần Thơ Hotel' then 'Vinpearl Hotel Cần Thơ'
          when propertyname = 'Vinpearl Beachfront Nha Trang' then 'Vinpearl Condotel Beachfront Nha Trang'
          when propertyname = 'Vinpearl Condotel Riverfront Da Nang' or propertyname = 'Vinpearl Condotel Riverfront Danang' then 'Vinpearl Condotel Riverfront Đà Nẵng'
          when propertyname = 'Vinpearl Condotel Phu Ly' then 'Vinpearl Condotel Phủ Lý'
          when propertyname = 'Vinpearl Hotel Tay Ninh' then 'Vinpearl Hotel Tây Ninh'
          when propertyname = 'Vinpearl Hotel Hue' then 'Vinpearl Hotel Huế'
          when propertyname = 'Vinpearl Luxury Da Nang' then 'Vinpearl Luxury Đà Nẵng'
          when propertyname = 'Vinpearl Golflink Nha Trang' then  'Vinpearl Discovery Golflink Nha Trang'
          when propertyname = 'Vinpearl Sealink Nha Trang' then 'Vinpearl Discovery Sealink Nha Trang'
          when propertyname = 'Vinpearl Wonderworld Phú Quốc' then 'Vinpearl Discovery Wonderworld Phú Quốc'
          when propertyname = 'Vinpearl Nha Trang Resort' then 'Vinpearl Resort Nha Trang'
          when propertyname = 'VinHolidays 1 Phú Quốc' then 'VinHolidays Fiesta Phú Quốc'
          when propertyname = 'Hòn Tằm Nha Trang' then 'Hòn Tằm Resort'
          else propertyname
     end as propertyname,
      lower(roomtypenames) roomtype
     from crs)

,crs2 as (
select  * except(detected), 
            case when detected in ('vi', 'VI') then 'VN'
            else detected
            end as detected,

from crs1
where RoomTypeCodes not in ('PM', 'PF') and propertyname in ('Vinpearl Resort & Spa Đà Nẵng',
'Vinpearl Resort & Spa Hội An',
'VinHolidays Fiesta Phú Quốc',
'Vinpearl Luxury Nha Trang',
'VinOasis Phú Quốc',
'Vinpearl Condotel Beachfront Nha Trang',
'Vinpearl Discovery Wonderworld Phú Quốc',
'Vinpearl Discovery Sealink Nha Trang',
'Vinpearl Discovery Golflink Nha Trang',
'Vinpearl Resort & Spa Hạ Long',
'Vinpearl Resort Nha Trang',
'Vinpearl Resort & Spa Nha Trang Bay',
'Vinpearl Resort & Golf Nam Hội An',
'Vinpearl Resort & Spa Phú Quốc',
'Hòn Tằm Resort'))

, crs3 as (
  select distinct 
    crs2.*, 
    NATION_GROUP, 
    nat_code.NATIONALITY, 
    case when code.rateplancodes in ('PR12108FB',  'PR12109FB', 'PR12108BB', 'PR12109FBB') then "FREE VINWONDERS" 
          else groupcode
          end as pro_cam
from crs2
left join `vp-dwh-prod-c827.MAPPING.NATION_CODE` nat_code on crs2.detected = nat_code.nation_code
left join (select distinct rateplancodes, groupcode from `vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_RATE_PLAN_CODES`) code on crs2.RatePlanCodes = code.rateplancodes)

,b2c_web_app as
 (select                          bookingdate,
                                  roomconfirmationnumber ordercode,
                                  distributionchannel as salechannel,
                                  "0" as channel,
                                  cast(null as string) membershipcode,
                                  cast(total as numeric) total,
                                  pro_cam groupname,
                                  propertyname hotelname,
                                  "HOTEL" as system
  from   crs3

  where true
  and status in ('Checkout', 'Inhouse', 'Reserved', 'Changed') 
  and channels = 'WEBSITE/APP'  
  and bookingdate >= '2023-01-01' 
  and (pro_cam is null or pro_cam not in ('TABS', 'CBNV','VINFAST'))
)


,b2c_vinpearl as
 (select                          date(booking_date,'Asia/Ho_Chi_Minh') bookingdate,
                                  room_confirmation_number ordercode,
                                  distribution_channel as salechannel,
                                  "0" as channel,
                                  --orderitemtype,
                                  cast(null as string) membershipcode,
                                  cast(total as numeric) total,
                                  case when Rate_Plan_codes in ('PR12108FB',  'PR12109FB', 'PR12108BB', 'PR12109FBB') then "FREE VINWONDERS"
                                       else UPPER(groupcode)
                                       end as groupname,
                                  property_name hotelname,
                                  "B2C VINPEARL" as system
  from   `vp-dwh-prod-c827.CIRRUS.res_booking__reservation`  hotel
  left join (select distinct rateplancodes, groupcode from `vp-dwh-prod-c827.VIN3S_DATATMART_VINPEARL.RATE_PLAN_CODES`) code
  on hotel.rate_plan_codes = code.rateplancodes
  where status in ('Checkout', 'Inhouse', 'Reserved', 'Changed') and organization not like '%melia%' and distribution_channel = 'VINPEARL JSC'  
  and date(booking_date,'Asia/Ho_Chi_Minh') >= '2023-01-01')

,final as (
select * from tour_final 
union all
select * from bundle1
union all
select * from b2c_web_app
union all
select * from b2c_vinpearl)


select distinct * from crs3

-- , newrates as(
-- select distinct
--   channels,
--   propertyname,
--   rateplancodes, 
--   rateplannames,
--   row_number() over(partition by rateplancodes order by length(rateplannames) desc) RN
-- from crs3
-- where true
-- and channels in ('WEBSITE/APP','OTAs') 
-- and status in ('Checkout', 'Inhouse', 'Reserved', 'Changed') 
-- and pro_cam is null
-- and rateplancodes not in (select distinct RatePlanCodes from `vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_RATE_PLAN_CODES`)
-- )
-- select * except(RN)
-- from newrates where RN = 1
-- order by 1,2,3,4
