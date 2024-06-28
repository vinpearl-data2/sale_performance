{{
    config(
        alias='ORDER_HOTEL_REPORT_COOK',
        materialized = 'table'
        )
}}

with
hotel_reservation as (
  select
    `vp-dwh-prod-c827.VINPEARL_TRAVEL.FUNC_VPT_BYTES_TO_GUID`(Id) AS ID,
    * except(rn, Id)
  from (
    select *, row_number() over(partition by ID order by updatedTime desc, processed_date desc) rn
    from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.HOTEL_RESERVATION`
  ) where rn = 1
),
hote_hotel as (
  select
    * except(rn)
  from (
    select `vp-dwh-prod-c827.VINPEARL_TRAVEL.FUNC_VPT_BYTES_TO_GUID`(Id) AS ID, Name, row_number() over(partition by Id order by updatedTime desc, processed_date desc) rn
    from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.HOTEL_HOTEL`
    union all
    select Id, Name, row_number() over(partition by Id order by ModifiedTime desc, processed_date desc) rn
    from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.HOTEL_PROPERTY`
  ) where rn = 1
),
markup_price as (
  select
    crsReservationId,
    sum(cast(cpi.markupExchangeToCash as float64)) as markupExchangeToCash
  from hotel_reservation hr
  cross join unnest(json_extract_array(hr.CustomPriceInfo)) as custom_price_info_array
  cross join unnest(
    [
      struct(
        json_extract_scalar(custom_price_info_array, "$.stayDate") as stayDate,
        json_extract_scalar(custom_price_info_array, "$.markupExchangeToCash") as markupExchangeToCash
      )
    ]
  ) cpi
  group by 1
),
order_orders as (
  select
    * except(rn)
  from (
    select *, row_number() over(partition by ID order by modifiedDate desc, processed_date desc) rn
    from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.ORDER_ORDERS`
  ) where rn = 1
),
order_orderitems as (
  select
    * except(rn)
  from (
    select *, row_number() over(partition by ID order by modifiedDate desc, processed_date desc) rn
    from `vp-data-lake-prod-c827.VINPEARL_TRAVEL.ORDER_ORDERITEMS`
  ) where rn = 1
),
tbl as (
  select
    distinct
    od.Id,
    odi.OrderId,
    od.OrderCode,
    od.TransactionId,
    od.PaymentType,
    od.PaymentStatus,
    od.Status as OrderStatus,
    od.SaleChannel,
    od.Channel,
    od.CreatedDate,
    concat(od.lastName, " ", od.firstName) as Fullname,
    od.PhoneNumber,
    od.Email,
    concat(profiles.lastName, " ", profiles.firstName) as FullnameCustomer,
    profiles.phoneNumber as PhoneNmberCustomer,
    profiles.email as EmailCustomer,
    case 
      when od.MembershipCode = "" then "GUEST"
      else od.MembershipCode
    end as MembershipCode,
    od.PromotionName,
    od.CouponValue,
    od.TotalPayment as TotalAmount,
    cast(0 as float64) as Surcharge,
    "" as SurchargeDetail,
    od.Note,
    hr.ArrivalDate as CheckInDate,
    hr.DepartureDate as CheckOutDate,
    hr.HotelId,
    rr.roomTypeRefID as RoomTypeId,
    rr.ratePlanRefID as RatePlanId,
    rr.ratePlanName as RatePlanName,
    rr.ratePlanCode as RatePlanCode,
    cast(roc.numberOfAdult as int) as AdultQuantity,
    cast(roc.numberOfChild as int) as ChildQuantity,
    profiles.Nationality,
    od.TotalPayment as PriceRoom,
    cast(cpi.markupExchangeToCash as float64) as MarkUpPriceRoom,
    hr.Total as SalePrice,
    hr.CreatedTime as CreatedAt,
    rr.RoomType,
    hh.Name as HotelName,
    od.AgentId,
    hr.CrsRoomConfirmationNumber as BookingId,
    rr.LengthOfStay,
    od.CouponCode,
    od.AccountantStatus,
    current_timestamp() as PROCESSED_DATE
  from order_orders od
  left join order_orderitems odi on od.id = odi.orderId
  inner join hotel_reservation hr on odi.productVariantId = hr.Id
  left join hote_hotel hh on hr.HotelId = hh.Id
  cross join unnest(json_extract_array(hr.Profiles)) as profiles_array
  cross join unnest(
    [struct(
      json_extract_scalar(profiles_array, '$.firstName') AS firstName,
      json_extract_scalar(profiles_array, '$.lastName') AS lastName,
      json_extract_scalar(profiles_array, '$.phoneNumber') AS phoneNumber,
      json_extract_scalar(profiles_array, '$.email') AS email,
      json_extract_scalar(profiles_array, '$.profileType') AS profileType,
      json_extract_scalar(profiles_array, '$.isPrimary') AS isPrimary,
      json_extract_scalar(profiles_array, '$.nationality') AS nationality
    )]
  ) as profiles
  cross join unnest(json_extract_array(hr.Roomrates)) as roomRates_array
  cross join unnest(
    [
      struct(
        json_extract_scalar(roomRates_array, "$.stayDate") as stayDate,
        json_extract_scalar(roomRates_array, "$.ratePlanRefID") as ratePlanRefID,
        json_extract_scalar(roomRates_array, "$.ratePlanCode") as ratePlanCode,
        json_extract_scalar(roomRates_array, "$.ratePlanName") as ratePlanName,
        json_extract_scalar(roomRates_array, "$.roomTypeRefID") as roomTypeRefID,
        json_extract_scalar(roomRates_array, "$.roomType.id") as roomTypeID,
        json_extract_scalar(roomRates_array, "$.roomType.name") as roomType,
        array_length(json_extract_array(hr.Roomrates)) as LengthOfStay
      )
    ]
  ) rr
  cross join unnest(
    [
      struct(
        json_extract_scalar(hr.RoomOccupancy, "$.numberOfAdult") as numberOfAdult,
        json_extract_scalar(hr.RoomOccupancy, "$.numberOfChild") as numberOfChild
        -- json_extract_scalar(hr.RoomOccupancy, "$.numberOfInfant") as numberOfInfant,
      )
    ]
  ) roc
  left join markup_price cpi on hr.CrsReservationId = cpi.CrsReservationId
  where profiles.profileType = 'Guest'
)

select 
  *
from tbl 
where true
