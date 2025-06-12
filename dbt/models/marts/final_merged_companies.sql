{{ config(materialized='table', alias='company_master_final') }}

select
  abn,
  entity_name,
  entity_type,
  entity_status,
  entity_address,
  entity_postcode,
  entity_state,
  start_date,
  company_name,
  industry
from {{ ref('matched_companies') }}
