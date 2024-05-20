
with tbl_new_ta_com AS (
        WITH tbl_ta_com AS (
            WITH tbl_tmp AS (
                SELECT
                    ConfirmationNumber,
                    TravelAgentProfileId,
                    CompanyProfileId,
                    MainGuestProfileId,
                    PropertyId,
                    ROW_NUMBER() OVER (
                        PARTITION BY (ConfirmationNumber)
                        ORDER BY
                            ModifiedAt DESC
                    ) AS cnt
                FROM
                    `vp-dwh-prod-c827`.`CIRRUS`.`prod_ops_front_office_Reservation`
            )
            SELECT
                ConfirmationNumber,
                TravelAgentProfileId AS AGENT_ID,
                CompanyProfileId AS COMPANY_ID,
                MainGuestProfileId AS MAIN_GUEST_ID,
                PropertyId
            FROM
                tbl_tmp
            WHERE
                cnt = 1
        ),
        TA_COMP_PROFILES AS (
            SELECT
                DISTINCT ProfileId,
                ProfileType,
                VerifiedProfileId
            FROM
                `vp-dwh-prod-c827`.`CIRRUS`.`PROFILE_ALL`
            WHERE
                ProfileType IN ('TravelAgent', 'Company')
                AND ProfileRecordOrigin = 'Verified'
        )
        SELECT
            tbl_ta_com.ConfirmationNumber,
            (
                CASE
                    WHEN prf.ACCOUNT IS NULL THEN 'VINPEARL'
                    WHEN prf.ACCOUNT = 'RESERVATION' THEN 'VINPEARL'
                    WHEN prf.ACCOUNT IN (
                        "VINPEARL - B2C",
                        "VINPEARL'S WEBSITE"
                    ) THEN 'WEBSITE'
                    WHEN prf.ACCOUNT IN (
                        "VINPEARL - PEARL CLUB"
                    ) THEN 'Đêm nghỉ miễn phí'
                    WHEN prf.ACCOUNT IN (
                        "MOBILE APP"
                    ) THEN 'MOBILE APP'
                    WHEN prf.ACCOUNT IN (
                        "ECOM - B2C",
                        "ECOM - B2B"
                    ) THEN 'Qũy BTB'
                    WHEN prf.ACCOUNT IN (
                        "VINPEARL - BUNDLE"
                    ) THEN '"VINPEARL - BUNDLE"'
                    WHEN prf.ACCOUNT IN (
                        "VINPEARL - B2B"
                    ) THEN "VINPEARL - B2B"
                    WHEN prf.ACCOUNT IN (
                        "VIETNAM AIRLINES B2B"
                    ) THEN NULL
                    WHEN prf.PROFILE_TYPE = 'OTAs & WEBSITE' THEN 'OTAs'
                    ELSE prf.PROFILE_TYPE
                END
            ) AS PROFILE_TYPE,
            prf.ACCOUNT
        FROM
            tbl_ta_com
            LEFT JOIN `vp-dwh-prod-c827`.`CIRRUS`.`prod_pms_property_pro_hotel` ho ON tbl_ta_com.propertyid = ho.id
            LEFT JOIN TA_COMP_PROFILES PTA ON tbl_ta_com.AGENT_ID = PTA.ProfileId
            AND PTA.ProfileType = 'TravelAgent'
            LEFT JOIN TA_COMP_PROFILES PC ON tbl_ta_com.COMPANY_ID = PC.ProfileId
            AND PC.ProfileType = 'Company'
            LEFT JOIN (
                SELECT
                    DISTINCT VerifiedProfileId
                FROM
                    TA_COMP_PROFILES
                WHERE
                    ProfileType = 'TravelAgent'
            ) PVTA ON tbl_ta_com.AGENT_ID = PVTA.VerifiedProfileId
            LEFT JOIN (
                SELECT
                    DISTINCT VerifiedProfileId
                FROM
                    TA_COMP_PROFILES
                WHERE
                    ProfileType = 'Company'
            ) PVC ON tbl_ta_com.COMPANY_ID = PVC.VerifiedProfileId
            LEFT JOIN (
                SELECT
                    DISTINCT MASTER_ACCOUNT ACCOUNT,
                    SERVER RESORT,
                    PROFILE_ID,
                    PROFILE_TYPE
                FROM
                    `vp-dwh-prod-c827`.`MAPPING`.`REVENUE_PROFILE_SIC`
            ) prf --USING (RESORT, PROFILE_ID)
            ON prf.RESORT = ho.code
            AND prf.PROFILE_ID = IFNULL(PTA.VerifiedProfileId, PVTA.VerifiedProfileId)
    )

,final1 as (
select booking_date, 
       checkin_date, 
       checkout_date, 
       date_diff(checkout_date, checkin_date, day) roomnights, 
       date_diff(checkin_date, booking_date, day) booking_window,
       resort, 
       destination,
       sum(total_revenue) total_rev,
       case when tbl.profile_type = 'OTAs' then 'OTAs'
            when tbl.profile_type in ('MOBILE APP', 'WEBSITE', '"VINPEARL - BUNDLE"') and account <> 'VIETNAM AIRLINES B2B' then 'Web/App'
            else 'VINPEARL'
       end as Channels,
       tbl.account Sub_channels,



from `vp-dwh-prod-c827.TARGETED_MARKETING.TM_STG_CR_PMS` pms
left join tbl_new_ta_com tbl on pms.resv_name_id = tbl.ConfirmationNumber

group by 1,2,3,4,5,6,7,9,10)


,final2 as (
select * from (
select final1.* except(destination),
         case when name = 'Vinpearl Sealink Nha Trang' then 'Vinpearl Discovery Sealink Nha Trang'
              when name = 'Vinpearl Golflink Nha Trang' then 'Vinpearl Discovery Golflink Nha Trang'
              when name = 'Vinpearl Wonderworld Phú Quốc' then 'Vinpearl Discovery Wonderworld Phú Quốc'
              when name = 'Vinpearl Beachfront Nha Trang' then 'Vinpearl Condotel Beachfront Nha Trang'
              else name
              end as propertyname,
              case when destination = 'Ngũ Hành Sơn' then 'Đà Nẵng'
                   when destination in ('Nam Hội An', 'Hội An') then 'Hội An'
                   else destination
                   end as destination


from final1
left join `vp-dwh-prod-c827.CIHMS.prod_pms_property_pro_hotel` ho on final1.resort = ho.code)
where checkin_date between "2023-01-01" and current_date('Asia/Ho_Chi_Minh') -1
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
'Hòn Tằm Resort'))

select distinct propertyname,destination, 
case when destination like '%Nha Trang%' or destination like '%Đà Nẵng%' or destination like '%Hội An%' then 'Region 1'
     when destination like '%Hạ Long%' or destination like '%Phú Quốc%' then 'Region 2'
     end as Region 
  

from final2
where propertyname is not null and destination is not null