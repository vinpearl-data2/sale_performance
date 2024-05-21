{{
  config(
    materialized='table'
  )
}}

with crs as (
  select 
    date(booking_date) BookingDate,
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
    CancelPenaltyPercent CancelPenaltyPercent

  from `vp-dwh-prod-c827.CIRRUS.res_booking__reservation`  
  where organization not like '%melia%'
)

,final as (
  select * except(roomtypenames, propertyname),
    case when distributionchannel in ("AGODA", "BCOM", 'BOOKING.COM',"CTRIP",'CTRIP_B2C',"EXPEDIA", "HOTELSCOMBINED","KLOOK_B2C",'KLOOK B2C', "PRESTIGIA","TIKET.COM",'TIKET_COM', "TRAVELOKA","HOTELBEDS_B2C", 'HOTELBEDS B2C',"INTERPARK","TIDESQUARE",'TIDESQUARE - OTA', 'DIVA TRAVEL', 'HBD') then "OTAs"
        --when sourcecode = "DR" then 'DIRECT'
        when distributionchannel = "WEBSITE" then 'WEBSITE/APP'
        when distributionchannel in ('RESERVATION', 'FRONT OFFICE') then 'DIRECT'
        when distributionchannel in ('VINGROUP P&L', 'VINPEARL JSC') or distributionchannel like '%KHACH LE TAP DOAN%' then 'VINGROUP'
        when distributionchannel = "ECOM - B2B" then 'BTB'
        when distributionchannel = "OWNER" then 'CBT'
        when distributionchannel = "VINPEARL – B2B" then 'VP TRAVEL'
        when distributionchannel = "VPT_BUNDLE" then 'VPT BUNDLE'
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
    case when propertyname = 'VinOasis Phu Quoc' then 'VinOasis Phú Quốc'
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
      when propertyname in ('StayNFun Ocean Park 2','Stay And Fun Ocean Park 2') then 'Stay And Fun Ocean Park'
      else propertyname
     end as propertyname,

 CASE
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"(VILLA)|(BIỆT THỰ)") THEN 
      (CASE
        WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"2\s*-*\s*BED|2\s*-*\s*bed|2\s*-*\s*Bed") THEN "VILLA 2 BED"
        WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"3\s*-*\s*BED|3\s*-*\s*bed|3\s*-*\s*Bed") THEN "VILLA 3 BED"
        WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"4\s*-*\s*BED|4\s*-*\s*bed|4\s*-*\s*Bed") THEN "VILLA 4 BED"
        WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"5\s*-*\s*BED|5\s*-*\s*bed|2\s*-*\s*Bed") THEN "VILLA 5 BED"
      ELSE
      "VILLA"
    END
      )
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"DELUXE|GRAND|deluxe|Deluxe|grand") THEN "DELUXE"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"DUPLEX|duplex|Duplex") THEN "SUITE"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"STUDIO|Studio|studio") THEN "STUDIO"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"SUITE|Suite|suite") THEN "SUITE"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"KING|King|king") THEN "SUITE"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"PRESIDENTIAL|presidental|Presidental") THEN "SUITE"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"BUSINESS|Business|business") THEN "SUITE"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"PREMIER|premier|Premier") THEN "SUITE"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"APARTMENT|Apartment|apartment") THEN "APARTMENT"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"POSTING MASTER|Posting master|posting master|Posting Master|PM") THEN "PM"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"PERMANENT FOLIO|Permanent folio|permanent folio|Permanent Folio") THEN "PF"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"STANDARD|Standard|standard") THEN "STUDIO"
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"CLUB") THEN "CLUB"
  ELSE
  'DELUXE'
END AS roomtype
   from crs
)

,final1 as (
  select * except (detected), 
    case when detected in ('Viet Nam', 'VI', 'Vietnamese', 'VN', 'Vietnam', 'Việt Nam', 'vi') then 'Vietnam'
        when detected in ('KR','South Korea','kr','South Korean') then 'South Korea'
        when detected in ('US', 'CA','AU') then 'USA-Canada-Aus'
        when detected is null or detected = '' then null
        else 'Others'
    end as detected
  from final
  where RoomTypeCodes not in ('PM', 'PF') 
  and propertyname in ('Vinpearl Resort & Spa Đà Nẵng',
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
                      'Stay And Fun Ocean Park'
                      )
)
select distinct channels from final1