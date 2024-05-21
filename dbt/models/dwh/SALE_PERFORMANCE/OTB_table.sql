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
    case when regexp_contains(propertyname,r'Phú Quốc|Phu Quoc') then 'Phú Quốc'
        when regexp_contains(propertyname,r'Hòn Tằm|Nha Trang') then 'Nha Trang'
        when regexp_contains(propertyname,r'Hội An|Hoi An') then 'Hội An'
        when regexp_contains(propertyname,r'Đà Nẵng|Da Nang|Da nang|Danang') then 'Đà Nẵng'
        when propertyname like '%Thanh Hóa%' then 'Thanh Hóa'
        when propertyname like '%Hải Phòng%' or propertyname like '%Hai Phong%' then 'Hải Phòng'
        when regexp_contains(propertyname,r'Hạ Long|Ha Long') then 'Hạ Long'
        when regexp_contains(propertyname,r'Phủ Lí|Phủ Lý|Phu Ly') then 'Phủ Lý'
        when propertyname like '%Lạng Sơn%' then 'Lạng Sơn'
        when propertyname like '%Cửa Hội%' or propertyname like '%Cửa Lò%' then 'Nghệ An'
        when propertyname like '%Hà Tĩnh%' then 'Hà Tĩnh'
        when regexp_contains(propertyname,r'Cần Thơ|Can Tho') then 'Cần Thơ'
        when regexp_contains(propertyname,r'Tây Ninh|Tay Ninh') then 'Tây Ninh'
        when regexp_contains(propertyname,r'Huế|Hue') then 'Huế'
        when propertyname like '%Landmark%' then 'Tp HCM'
        when regexp_contains(propertyname,r'Quảng Bình|Quang Binh') then 'Quảng Bình'
        when regexp_contains(propertyname,'Stay And Fun|StayNFun') then 'Hà Nội'
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
          when regexp_contains(propertyname, r'Stay And Fun Ocean Park|StayNFun Ocean Park') then 'Stay And Fun Ocean Park'
          else propertyname
    end as propertyname,
    lower(roomtypenames) roomtype,
    case when detected in ('vi', 'VI') then 'VN'
         else detected
    end as detected
  from crs
  where RoomTypeCodes not in ('PM', 'PF')
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
    from `vp-dwh-prod-c827.VINPEARL_TRAVEL.VIN3S_DM_VP_RATE_PLAN_CODES`
  ) code on crs.RatePlanCodes = code.rateplancodes
  where crs.propertyname in ('Vinpearl Resort & Spa Đà Nẵng',
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
                            'Hòn Tằm Resort')
  or regexp_contains(crs.propertyname, r'Stay And Fun|StayNFun')
)

select distinct * from crs_final