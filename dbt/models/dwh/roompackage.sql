with a as (
select * except(sourcecode,detected, propertyname),
         case when distributionchannel in ("AGODA", 
        "BCOM","CTRIP","EXPEDIA", "HOTELSCOMBINED","KLOOK_B2C","PRESTIGIA","TIKET_COM","TRAVELOKA","HOTELBEDS_B2C","INTERPARK","TIDESQUARE", 'DIDA TRAVEL', 'HBD') then "OTAs"
              --when sourcecode = "DR" then 'DIRECT'
              when distributionchannel = "WEBSITE" then 'WEBSITE'
              when distributionchannel = 'OWNER' then 'APP'
              else null
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

          case
          when detected in ('Brunei Darussalam','Cambodia', 'Indonesia',"Lao People's Democratic Republic",'Malaysia','Myanmar','Philippines','Singapore','Thailand') then 'ASEAN'
          when detected in ('Canada', 'United States of America','United States', 'Virgin Islands (U.S.)') then 'US-Canada'
          when detected = "Korea (Democratic People's Republic of)" then  'North Korea'
          when detected ='Korea, Republic of'then 'South Korea'
          when detected = 'Russian Federation' then 'Russia'
          when detected in ('Japan', 'Japanese') then 'Japan'
          else detected 
    end as detected, `vp-dwh-prod-c827.UP_CROSS_SELL.RoomPackage`(rateplancodes) roompackage,
    case  when propertyname = 'VinOasis Phu Quoc' then 'VinOasis Phú Quốc'
          when propertyname = '빈펄 럭셔리 랜드마크 81' then 'Vinpearl Luxury Landmark 81'
          when propertyname in ('Vinpearl Resort và Golf Nam Hội An', 'Vinpearl Resort & Golf Nam Hoi An') then 'Vinpearl Resort & Golf Nam Hội An'
          when propertyname in ('Vinpearl Resort & Spa Hoi An', "Vinpearl Resort và Spa Hội An") then "Vinpearl Resort & Spa Hội An"
          when propertyname in ('Vinpearl Resort và Golf Phú Quốc', "Vinpearl Resort and Golf Phu Quoc") then "Vinpearl Resort & Golf Phú Quốc"
          when propertyname = "Vinpearl Resort và Spa Đà Nẵng" then "Vinpearl Resort & Spa Đà Nẵng"
          when propertyname = "Vinpearl Resort và Spa Hạ Long" then "Vinpearl Resort & Spa Hạ Long"
          when propertyname = "Vinpearl Resort và Spa Long Beach Nha Trang" then "Vinpearl Resort & Spa Long Beach Nha Trang"
          when propertyname = "Vinpearl Resort và Spa Nha Trang Bay" then "Vinpearl Resort & Spa Nha Trang Bay"
          when propertyname in  ("Vinpearl Resort và Spa Phú Quốc", "Vinpearl Resort & Spa Phu Quoc") then "Vinpearl Resort & Spa Phú Quốc"
          when propertyname = 'Vinpearl Hotel Quang Binh' then 'Vinpearl Hotel Quảng Bình'
          when propertyname = 'VinHoliday 1 Phú Quốc' then 'VinHolidays 1 Phú Quốc'
          when propertyname = 'Vinpearl Hotel Rivera Hai Phong' then 'Vinpearl Hotel Rivera Hải Phòng'
          when propertyname = 'Vinpearl Hotel Imperia Hai Phong' then 'Vinpearl Hotel Imperia Hải Phòng'
          else propertyname
     end as propertyname,

 CASE
    WHEN REGEXP_CONTAINS(`vp-dwh-prod-c827.UP_CROSS_SELL.normalize_string`(roomtypenames), r"(VILLA)|(BIỆT THỰ)") THEN 
    ( CASE
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


from `vp-dwh-dev-d963.test_dataset_sgp.CRS_RESERVATION`)

,final as (
select * except (detected), case when detected in ('Viet Nam', 'VI') then 'Vietnam'
                                 when detected = 'South Korea' then 'South Korea'
                                 when detected in ('US-Canada', 'Australia') then 'USA-Canada-Aus'
                                 else 'Others'
                                 end as detected
from a
where roomtype not in ('PM', 'PF') and propertyname in ('Vinpearl Resort & Spa Đà Nẵng',
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
'Vinpearl Resort & Spa Phú Quốc'))

select distinct roompackage

from final