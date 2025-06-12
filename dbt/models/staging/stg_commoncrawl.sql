{{ config(materialized='table') }}

with raw as (
  select
    company_name,
    industry
  from {{ source('public', 'commoncrawl_companies') }}
),

cleaned as (
  select
    lower(trim(regexp_replace(company_name, '[^a-zA-Z0-9 ]', '', 'g'))) as raw_company,
    lower(trim(regexp_replace(industry, '[^a-zA-Z0-9 ]', '', 'g'))) as raw_industry
  from raw
),

split_words as (
  select
    raw_company,
    raw_industry,
    string_to_array(raw_company, ' ') as company_words,
    string_to_array(raw_industry, ' ') as industry_words
  from cleaned
),

limited as (
  select
    array_to_string(company_words[1:20], ' ') as company_name_cleaned,
    array_to_string(industry_words[1:20], ' ') as industry_cleaned
  from split_words
)

select
  initcap(company_name_cleaned) as company_name,
  initcap(industry_cleaned) as industry
from limited
where company_name is not null
