{{
  config(
    materialized='table'
  )
}}

with 
PAYMENT as (
    select sale_date, a.order_code, coupon_code, coupon_value, total_payment,sub_total, order_item, pearls, channels, sale_channel from 
    
    (select distinct date(created_date) sale_date, 
                     order_code, 
                     coupon_code,
                     case when channel =  1 then 'Online Website'
                          when channel =  2 then 'Online App'
                          when channel =  3 then 'Online Chatbot'
                          when channel = 4 then 'Offline Website'
                          when channel = 10 then 'Booking VinWonder'
                          when channel = 11 then 'VinWonder App'
                          end as CHANNELS,
                          sum(coupon_value) coupon_value, 
                          sum(sub_total) sub_total, 
                          sum(total_payment) total_payment,
                          sale_channel,
                     case when lower(membership_code) like '%pearl%' then membership_code
                           else 'NON PEARL'
                           end as Pearls 

    from `vp-dwh-prod-c827`.`VINPEARL_TRAVEL`.`ORDER_ORDERS` OD
    where payment_status = 2 and total_payment <> 0.0
    group by 1,2,3,4,8, 9) a
    left join
    (select distinct order_code, sum(discount_value) discount_value, string_agg(cast(ORDER_ITEM_TYPE as string) order by created_date asc)  ORDER_ITEM,
    FROM
    (select * from `vp-dwh-prod-c827`.`VINPEARL_TRAVEL`.`ORDER_ORDERS`  where total_payment <> 0.0 and payment_status = 2) OD
    LEFT JOIN
    `vp-dwh-prod-c827`.`VINPEARL_TRAVEL`.`ORDER_ORDERITEMS` ODI
    ON ODI.ORDER_ID = OD.ID
    group by order_code) b on a.order_code = b.order_code)

,final1 as (
  select distinct sale_date, 
                  order_code,
                  case when trim(coupon_code) = '' then null
                       when coupon_code is null then null
                       else coupon_code
                  end as coupon_code, 
                  coupon_value, 
                  total_payment, 
                  sub_total, 
                  pearls,
                  channels,
                  case when sale_channel = 'online' then 'Online'
                       when sale_channel = 'corporation' then 'Công ty'
                       when sale_channel = 'customerservice' then 'Chăm sóc khách hàng'
                       when sale_channel = 'property' then 'Cơ sở'
                       when sale_channel = 'internal' then 'Nội bộ'
                       when sale_channel = 'travelagent' then 'Đại lý'
                       when sale_channel = 'b2b' then 'B2B'
                  end as sale_channels,
                  case when order_item like '%4%' and order_item like '%3%' then 'BUNDLE'
                       when order_item like '%5%' then 'VINWONDER'
                       when order_item like '%4%' then 'HOTEL'
                       when order_item like '%3%' then 'FLIGHT'
                       when order_item like '%2%' then 'TOUR'
                       when order_item like '%1%' then 'VOUCHER'
                       when order_item like '%7%' then 'VINWONDER_TOUR'
                  END AS ORDER_ITEM_GROUP,
                  from payment)

select distinct * from final1

--select distinct * from vp-dwh-prod-c827.VINPEARL_TRAVEL.PROMOTION_PROMOTIONCODES