{{ config(materialized='table') }}

with raw as (
  select
    abn,
    lower(trim(entity_name)) as entity_name,
    initcap(trim(entity_type)) as entity_type,
    initcap(trim(entity_status)) as entity_status,
    initcap(trim(entity_address)) as entity_address,
    entity_postcode,
    upper(trim(entity_state)) as entity_state,
    to_date(entity_start_date, 'YYYY-MM-DD') as start_date
  from {{ source('public', 'abn_companies') }}
)

select distinct *
from raw
where abn is not null and entity_name is not null
