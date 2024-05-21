{{
  config(
    materialized='table'
  )
}}

-- dim channel
select distinct 
  case when Distribution_Channel in ("AGODA", "BCOM", 'BOOKING.COM',"CTRIP",'CTRIP_B2C',"EXPEDIA", "HOTELSCOMBINED","KLOOK_B2C",'KLOOK B2C', "PRESTIGIA","TIKET.COM",'TIKET_COM', "TRAVELOKA","HOTELBEDS_B2C", 'HOTELBEDS B2C',"INTERPARK","TIDESQUARE",'TIDESQUARE - OTA', 'DIVA TRAVEL', 'HBD') then "OTAs"
    when Distribution_Channel = "WEBSITE" then 'WEBSITE/APP'
    when Distribution_Channel in ('RESERVATION', 'FRONT OFFICE') then 'DIRECT'
    when Distribution_Channel in ('VINGROUP P&L', 'VINPEARL JSC') or Distribution_Channel like '%KHACH LE TAP DOAN%' then 'VINGROUP'
    when Distribution_Channel = "ECOM - B2B" then 'BTB'
    when Distribution_Channel = "OWNER" then 'CBT'
    when Distribution_Channel = "VINPEARL - B2B" then 'VP TRAVEL'
    when Distribution_Channel = "VPT_BUNDLE" then 'VPT BUNDLE'
    else 'TA'
  end as Channels,
from `vp-dwh-prod-c827.CIRRUS.res_booking__reservation`  
where organization not like '%melia%'