{{ config(materialized='view') }}

with source_data as (
  select * from {{ source('raw_data', 'facebook_ads_windsor_campaigns') }}
),

unique_campaigns as (
  select 
    account_id,
    campaign_id,
    coalesce(max(campaign), max(case when campaign is not null then campaign end)) as campaign_name,
    coalesce(max(campaign_bid_strategy), max(case when campaign_bid_strategy is not null then campaign_bid_strategy end)) as campaign_bid_strategy,
    max(campaign_budget_remaining) as campaign_budget_remaining,
    coalesce(max(campaign_buying_type), max(case when campaign_buying_type is not null then campaign_buying_type end)) as campaign_buying_type,
    coalesce(max(campaign_configured_status), max(case when campaign_configured_status is not null then campaign_configured_status end)) as campaign_configured_status,
    max(campaign_created_time) as campaign_created_time,
    max(campaign_daily_budget) as campaign_daily_budget,
    coalesce(max(campaign_effective_status), max(case when campaign_effective_status is not null then campaign_effective_status end)) as campaign_effective_status,
    max(campaign_lifetime_budget) as campaign_lifetime_budget,
    coalesce(max(campaign_objective), max(case when campaign_objective is not null then campaign_objective end)) as campaign_objective,
    coalesce(max(campaign_special_ad_category), max(case when campaign_special_ad_category is not null then campaign_special_ad_category end)) as campaign_special_ad_category,
    max(campaign_spend_cap) as campaign_spend_cap,
    max(campaign_start_time) as campaign_start_time,
    max(campaign_stop_time) as campaign_stop_time,
    coalesce(max(objective), max(case when objective is not null then objective end)) as objective,
    sum(spend) as spend,
    sum(totalcost) as totalcost
  from source_data
  where campaign_id is not null
    and campaign_id != ''
    and account_id is not null
    and account_id != ''
  group by account_id, campaign_id
),

cleaned_campaigns as (
  select
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as campaign_key,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_key,
    
    cast(campaign_id as string) as campaign_id,
    cast(account_id as string) as account_id,
    
    -- Campaign name cleaning and standardization
    case 
      when trim(campaign_name) = '' or campaign_name is null then 'Unknown Campaign'
      when regexp_contains(trim(campaign_name), r'^[0-9]+$') then concat('Campaign ', trim(campaign_name))
      else trim(regexp_replace(campaign_name, r'\s+', ' '))
    end as campaign_name_clean,
    
    cast(campaign_name as string) as campaign_name_raw,
    
    -- Campaign objective standardization (prioritize campaign_objective, fallback to objective)
    case 
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'LEAD_GENERATION' then 'Lead Generation'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'CONVERSIONS' then 'Conversions'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'TRAFFIC' then 'Traffic'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'BRAND_AWARENESS' then 'Brand Awareness'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'REACH' then 'Reach'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'VIDEO_VIEWS' then 'Video Views'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'MESSAGES' then 'Messages'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'APP_INSTALLS' then 'App Installs'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'EVENT_RESPONSES' then 'Event Responses'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'LINK_CLICKS' then 'Link Clicks'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'LOCAL_AWARENESS' then 'Local Awareness'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'OFFER_CLAIMS' then 'Offer Claims'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'PAGE_LIKES' then 'Page Likes'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'POST_ENGAGEMENT' then 'Post Engagement'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'PRODUCT_CATALOG_SALES' then 'Product Catalog Sales'
      when upper(trim(coalesce(campaign_objective, objective, ''))) = 'STORE_VISITS' then 'Store Visits'
      else coalesce(campaign_objective, objective, 'Unknown')
    end as campaign_objective_clean,
    
    cast(coalesce(campaign_objective, objective) as string) as campaign_objective_raw,
    
    -- Campaign status standardization (prioritize effective status)
    case 
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) in ('ACTIVE', 'LEARNING', 'LEARNING LIMITED') then 'ACTIVE'
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) = 'PAUSED' then 'PAUSED'
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) in ('ARCHIVED', 'DELETED') then 'ARCHIVED'
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) = 'SCHEDULED' then 'SCHEDULED'
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) in ('UNDER REVIEW', 'PENDING REVIEW', 'IN REVIEW') then 'UNDER_REVIEW'
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) in ('REJECTED', 'DISAPPROVED') then 'REJECTED'
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) in ('ERROR', 'NO DELIVERY', 'NOT DELIVERING', 'LIMITED DELIVERY') then 'ERROR'
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) = 'DRAFT' then 'DRAFT'
      when upper(trim(coalesce(campaign_effective_status, campaign_configured_status, ''))) = 'COMPLETED' then 'COMPLETED'
      else 'UNKNOWN'
    end as campaign_status_clean,
    
    cast(coalesce(campaign_effective_status, 'Unknown') as string) as campaign_effective_status,
    cast(coalesce(campaign_configured_status, 'Unknown') as string) as campaign_configured_status,
    
    -- Campaign metadata and properties
    cast(campaign_bid_strategy as string) as campaign_bid_strategy,
    cast(campaign_buying_type as string) as campaign_buying_type,
    cast(campaign_special_ad_category as string) as campaign_special_ad_category,
    
    -- Budget and spend fields
    cast(coalesce(campaign_daily_budget, 0.0) as float64) as campaign_daily_budget,
    cast(coalesce(campaign_lifetime_budget, 0.0) as float64) as campaign_lifetime_budget,
    cast(coalesce(campaign_budget_remaining, 0.0) as float64) as campaign_budget_remaining,
    cast(coalesce(campaign_spend_cap, 0.0) as float64) as campaign_spend_cap,
    cast(coalesce(spend, totalcost, 0.0) as float64) as amount_spent,
    cast(coalesce(totalcost, spend, 0.0) as float64) as total_cost,
    
    -- Date fields
    cast(campaign_created_time as timestamp) as campaign_created_time,
    cast(campaign_start_time as timestamp) as campaign_start_time,
    cast(campaign_stop_time as timestamp) as campaign_stop_time,
    
    -- Campaign quality flags
    case 
      when lower(trim(campaign_name)) like '%test%' 
        or lower(trim(campaign_name)) like '%demo%'
        or lower(trim(campaign_name)) like '%sample%'
        or lower(trim(campaign_name)) like '%trial%'
        or regexp_contains(lower(trim(campaign_name)), r'\b(test|demo|sample|trial)\b')
      then true
      else false
    end as is_test_campaign,
    
    case 
      when campaign_name is null or trim(campaign_name) = '' then 'Missing Name'
      when regexp_contains(trim(campaign_name), r'^[0-9]+$') then 'Numeric Only'
      when length(trim(campaign_name)) < 3 then 'Too Short'
      else 'Valid'
    end as campaign_name_quality_flag,
    
    -- Budget type classification
    case 
      when coalesce(campaign_daily_budget, 0) > 0 and coalesce(campaign_lifetime_budget, 0) = 0 then 'Daily Budget'
      when coalesce(campaign_lifetime_budget, 0) > 0 and coalesce(campaign_daily_budget, 0) = 0 then 'Lifetime Budget'
      when coalesce(campaign_daily_budget, 0) > 0 and coalesce(campaign_lifetime_budget, 0) > 0 then 'Both Budgets'
      else 'No Budget Set'
    end as budget_type,
    
    current_timestamp() as _dbt_loaded_at
    
  from unique_campaigns
)

select * from cleaned_campaigns
where campaign_id is not null
  and campaign_id != ''
  and account_id is not null
  and account_id != ''
order by campaign_name_clean