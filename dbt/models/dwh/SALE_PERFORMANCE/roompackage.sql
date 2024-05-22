{{
  config(
    materialized='table'
  )
}}

select distinct roompackage

from {{ ref('OTB_table')}}