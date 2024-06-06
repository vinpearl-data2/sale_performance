{{
  config(
    materialized='table'
  )
}}

with crs_ref as (
  select 
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
  from `vp-dwh-prod-c827.CIHMS.res_booking__reservation`
)

, crs_ref_vpt as (
  select
    cr.id,
    cr.Room_Confirmation_Number,
    cr.reservation_reference_id,
    coalesce(cr.order_code,o.OrderCode) order_code
  from crs_ref cr
  left join `vp-dwh-prod-c827.VINPEARL_TRAVEL.HOTEL_RESERVATION` hr on hr.Opera_Confirmation_Number = cr.crs_code
  left join `vp-data-lake-prod-c827.VINPEARL_TRAVEL.ORDER_ORDERITEMS` oi on oi.ProductVariantId = hr.ID
  left join `vp-data-lake-prod-c827.VINPEARL_TRAVEL.ORDER_ORDERS` o on o.ID = oi.OrderId
  where coalesce(cr.order_code,hr.Opera_Confirmation_Number) like 'VPT-%'
)

, crs as (
  select
    date(r.booking_date,'Asia/Ho_Chi_Minh') BookingDate,
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
  where r.organization not like '%melia%'
)

, crs_property as (
  select * except (propertyName, detected),
    case when distributionchannel in ("AGODA", "BCOM", 'BOOKING.COM',"CTRIP",'CTRIP_B2C',"EXPEDIA", "HOTELSCOMBINED","KLOOK_B2C",'KLOOK B2C', "PRESTIGIA","TIKET.COM",'TIKET_COM', "TRAVELOKA","INTERPARK","TIDESQUARE",'TIDESQUARE - OTA', 'DIDA TRAVEL', 'HBD', 'MAKEMYTRIP B2C') then "OTAs"
        when distributionchannel = "WEBSITE" then 'WEBSITE/APP'
        when distributionchannel in ('RESERVATION', 'FRONT OFFICE') then 'DIRECT'
        when distributionchannel in ('VINGROUP P&L', 'VINPEARL JSC') or distributionchannel like '%KHACH LE TAP DOAN%' then 'VINGROUP'
        when distributionchannel = "ECOM - B2B" then 'WEBSITE/APP'
        when distributionchannel = "OWNER" then 'CBT'
        when distributionchannel = "VINPEARL - B2B" then 'WEBSITE/APP'
        when distributionchannel = "VPT_BUNDLE" then 'WEBSITE/APP'
        else 'TA'
    end as Channels,
    `vp-dwh-prod-c827.UP_CROSS_SELL.RoomPackage`(rateplancodes) roompackage,
    case  when regexp_contains(propertyname, r'Hòn Tằm') then 'Hòn Tằm Resort Nha Trang'
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
    end as propertyname,
    lower(roomtypenames) roomtype,
    case when detected in ('vi', 'VI') then 'VN'
         else detected
    end as detected
  from crs
  where RoomTypeCodes not in ('PM', 'PF')
  and regexp_contains(propertyname, r'Melia|Mariot') is false
)

, crs_final as (
  select distinct 
    crs.*, 
    NATION_GROUP, 
    nat_code.NATIONALITY, 
    case when code.rateplancodes in ('PR12108FB',  'PR12109FB', 'PR12108BB', 'PR12109FBB') then "FREE VINWONDERS" 
         else groupcode
    end as pro_cam
  from crs_property crs
  left join `vp-dwh-prod-c827.MAPPING.NATION_CODE` nat_code on crs.detected = nat_code.nation_code
  left join (
    select distinct rateplancodes, groupcode 
    from `vp-dwh-prod-c827.VIN3S_DATATMART_VINPEARL.RATE_PLAN_CODES`
  ) code on crs.RatePlanCodes = code.rateplancodes
  where propertyname is not null
)

select distinct 
  BookingDate,
  ArrivalDate,
  DepartureDate,
  Status,
  PropertyCode,
  ItineraryNumber,
  RoomConfirmationNumber,
  CancellationNumber,
  DistributionChannel,
  RoomTypeCodes,
  RoomTypeNames,
  RatePlanCodes,
  RatePlanNames,
  SourceCode,
  MarketCode,
  NumberOdAdults,
  NumberOfOtherOccupancies,
  MainGuest,
  GuestEmail,
  GuestPhone,
  Address,
  TravelAgent,
  IncludePackage,
  BookablePackage,
  BookablePackageAmount,
  Booker,
  BookerEmail,
  BookerPhone,
  RoomNights,
  Notes,
  Total,
  CancelPenaltyAmount,
  CancelPenaltyPercent,
  order_code,
  Channels,
  case when propertyname like '%Nha Trang%' then 'Nha Trang'
       when propertyname like '%Hội An%' then 'Hội An'
       when propertyname like '%Phú Quốc%' then 'Phú Quốc'
       when propertyname like '%Hạ Long%' then 'Hạ Long'
       when propertyname like '%Hòn Tằm%' then 'Nha Trang'
       when propertyname = 'StayNFun Ocean Park 2' then 'Hà Nội'
  end as destination,
  roompackage,
  propertyname,
  roomtype,
  detected,
  NATION_GROUP,
  NATIONALITY,
  pro_cam,
from crs_final