{{ config(
    materialized='incremental',
    unique_key='insights_key',
    on_schema_change='fail',
    partition_by={
        "field": "date_day",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['account_id', 'campaign_id']
) }}

with source_data as (
  select 
    date,
    account_id,
    account_name,
    account_currency,
    campaign_id,
    campaign,
    campaign_objective,
    campaign_status,
    ad_id,
    ad_name,
    impressions,
    clicks,
    spend,
    reach,
    frequency,
    cpm,
    cpc,
    ctr,
    actions_purchase,
    action_values_purchase
  from {{ source('raw_data', 'facebook_ads_windsor_insights') }}
  where date is not null
    and account_id is not null
    and campaign_id is not null
    and ad_id is not null
    {% if is_incremental() %}
      and date > (select max(date_day) from {{ this }})
    {% endif %}
),

deduplicated_data as (
  select 
    date,
    account_id,
    account_name,
    account_currency,
    campaign_id,
    campaign,
    campaign_objective,
    campaign_status,
    ad_id,
    ad_name,
    impressions,
    clicks,
    spend,
    reach,
    frequency,
    cpm,
    cpc,
    ctr,
    actions_purchase,
    action_values_purchase
  from source_data
  qualify row_number() over (
    partition by date, account_id, campaign_id, ad_id 
    order by 
      coalesce(spend, 0) desc, 
      coalesce(impressions, 0) desc,
      ad_name desc -- deterministic ordering for ties
  ) = 1
),

cleaned_data as (
  select
    -- Surrogate key for the grain: date, account_id, campaign_id, ad_id
    {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id', 'ad_id']) }} as insights_key,
    
    -- Core identifiers with BigQuery data type casting and null handling
    cast(date as date) as date_day,
    cast(coalesce(account_id, '') as string) as account_id,
    cast(coalesce(account_name, '') as string) as account_name,
    upper(trim(cast(coalesce(account_currency, 'USD') as string))) as account_currency,
    cast(coalesce(campaign_id, '') as string) as campaign_id,
    cast(coalesce(campaign, '') as string) as campaign_name,
    
    -- Campaign objective standardization per field mapping doc
    case 
      when upper(trim(coalesce(campaign_objective, ''))) = 'LEAD_GENERATION' then 'Lead Generation'
      when upper(trim(coalesce(campaign_objective, ''))) = 'CONVERSIONS' then 'Conversions'
      when upper(trim(coalesce(campaign_objective, ''))) = 'TRAFFIC' then 'Traffic'
      when upper(trim(coalesce(campaign_objective, ''))) = 'BRAND_AWARENESS' then 'Brand Awareness'
      when upper(trim(coalesce(campaign_objective, ''))) = 'REACH' then 'Reach'
      when upper(trim(coalesce(campaign_objective, ''))) = 'VIDEO_VIEWS' then 'Video Views'
      when upper(trim(coalesce(campaign_objective, ''))) = 'MESSAGES' then 'Messages'
      when upper(trim(coalesce(campaign_objective, ''))) = 'APP_INSTALLS' then 'App Installs'
      when upper(trim(coalesce(campaign_objective, ''))) = 'EVENT_RESPONSES' then 'Event Responses'
      when upper(trim(coalesce(campaign_objective, ''))) = 'LINK_CLICKS' then 'Link Clicks'
      when upper(trim(coalesce(campaign_objective, ''))) = 'LOCAL_AWARENESS' then 'Local Awareness'
      when upper(trim(coalesce(campaign_objective, ''))) = 'OFFER_CLAIMS' then 'Offer Claims'
      when upper(trim(coalesce(campaign_objective, ''))) = 'OUTCOME_APP_PROMOTION' then 'Outcome App Promotion'
      when upper(trim(coalesce(campaign_objective, ''))) = 'OUTCOME_AWARENESS' then 'Outcome Awareness'
      when upper(trim(coalesce(campaign_objective, ''))) = 'OUTCOME_ENGAGEMENT' then 'Outcome Engagement'
      when upper(trim(coalesce(campaign_objective, ''))) = 'OUTCOME_LEADS' then 'Outcome Leads'
      when upper(trim(coalesce(campaign_objective, ''))) = 'OUTCOME_SALES' then 'Outcome Sales'
      when upper(trim(coalesce(campaign_objective, ''))) = 'OUTCOME_TRAFFIC' then 'Outcome Traffic'
      when upper(trim(coalesce(campaign_objective, ''))) = 'PAGE_LIKES' then 'Page Likes'
      when upper(trim(coalesce(campaign_objective, ''))) = 'POST_ENGAGEMENT' then 'Post Engagement'
      when upper(trim(coalesce(campaign_objective, ''))) = 'PRODUCT_CATALOG_SALES' then 'Product Catalog Sales'
      when upper(trim(coalesce(campaign_objective, ''))) = 'STORE_VISITS' then 'Store Visits'
      else coalesce(campaign_objective, 'Unknown')
    end as campaign_objective,
    
    cast(coalesce(campaign_status, 'Unknown') as string) as campaign_status,
    cast(coalesce(ad_id, '') as string) as ad_id,
    cast(coalesce(ad_name, '') as string) as ad_name,
    
    -- Performance metrics with BigQuery casting and validation
    cast(coalesce(impressions, 0) as int64) as impressions,
    cast(coalesce(clicks, 0) as int64) as clicks,
    cast(coalesce(spend, 0.0) as float64) as spend,
    cast(coalesce(reach, 0) as int64) as reach,
    cast(coalesce(frequency, 0.0) as float64) as frequency,
    
    -- Cost metrics with null handling
    cast(coalesce(cpm, 0.0) as float64) as cost_per_mille,
    cast(coalesce(cpc, 0.0) as float64) as cost_per_click,
    coalesce(safe_cast(ctr as float64), 0.0) as click_through_rate,
    
    -- Conversion metrics
    coalesce(safe_cast(actions_purchase as int64), 0) as conversions,
    coalesce(safe_cast(action_values_purchase as float64), 0.0) as conversion_value,
    
    -- Calculated metrics with proper null handling
    case 
      when coalesce(safe_cast(actions_purchase as int64), 0) > 0 then spend / safe_cast(actions_purchase as int64)
      else null
    end as cost_per_conversion,
    
    case 
      when coalesce(spend, 0) > 0 then safe_cast(action_values_purchase as float64) / spend
      else null
    end as return_on_ad_spend,
    
    -- Data quality flags
    case 
      when date is null or account_id is null or campaign_id is null or ad_id is null then 'Missing Key Fields'
      when spend < 0 or clicks < 0 or impressions < 0 then 'Negative Metrics'
      when clicks > impressions and impressions > 0 then 'Invalid CTR'
      when frequency < 0 then 'Invalid Frequency'
      else 'Valid'
    end as data_quality_flag,
    
    current_timestamp() as _dbt_loaded_at
    
  from deduplicated_data
  where date is not null
    and account_id is not null
    and campaign_id is not null
    and ad_id is not null
)

select * from cleaned_data
where 1=1
  -- Date filtering
  and date_day >= '{{ var("facebook_ads_start_date") }}'
  
  -- Data quality filtering
  and data_quality_flag = 'Valid'
  
  -- Test campaign filtering with comprehensive patterns
  {% if var("exclude_test_campaigns", true) %}
  and not (
    lower(campaign_name) like '%test%' 
    or lower(campaign_name) like '%demo%'
    or lower(campaign_name) like '%sample%'
    or lower(campaign_name) like '%trial%'
    or lower(ad_name) like '%test%'
    or lower(ad_name) like '%demo%'
    or lower(ad_name) like '%sample%'
    or lower(ad_name) like '%trial%'
    or regexp_contains(lower(campaign_name), r'\b(test|demo|sample|trial)\b')
    or regexp_contains(lower(ad_name), r'\b(test|demo|sample|trial)\b')
  )
  {% endif %}
  
  -- Spend and performance thresholds
  and spend >= {{ var("min_spend_threshold", 0) }}
  and impressions >= {{ var("min_impressions_threshold", 1) }}
  
  -- Additional data validation filters
  and account_id != ''
  and campaign_id != ''
  and ad_id != ''
  and campaign_name != ''
  and ad_name != ''