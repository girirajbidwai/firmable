{{ config(materialized='table') }}

select
  abn.abn,
  abn.entity_name,
  abn.entity_type,
  abn.entity_status,
  abn.entity_address,
  abn.entity_postcode,
  abn.entity_state,
  abn.start_date,
  cc.company_name,
  cc.industry
from {{ ref('stg_commoncrawl') }} cc
join {{ ref('stg_abn') }} abn
  on abn.entity_name ilike '%' || cc.company_name || '%'
