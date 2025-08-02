{{ config(materialized='view') }}

with base_data as (
  select 
    -- Core identifiers
    insights_key,
    date_day,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_objective,
    campaign_status,
    ad_id,
    ad_name,
    
    -- Raw metrics
    impressions,
    clicks,
    spend,
    reach,
    frequency,
    conversions,
    conversion_value,
    
    -- Currency and exchange rate information
    account_currency,
    exchange_rate,
    exchange_rate_date,
    exchange_rate_source,
    exchange_rate_quality_flag,
    
    -- Original currency amounts
    spend_original_currency,
    conversion_value_original_currency,
    cost_per_click_original_currency,
    cost_per_mille_original_currency,
    cost_per_conversion_original_currency,
    
    -- USD converted amounts
    spend_usd,
    conversion_value_usd,
    cost_per_click_usd,
    cost_per_mille_usd,
    cost_per_conversion_usd,
    
    -- Existing calculated fields from staging (now from currency normalized)
    cost_per_click as existing_cost_per_click,
    cost_per_mille as existing_cost_per_mille,
    click_through_rate as existing_click_through_rate,
    cost_per_conversion as existing_cost_per_conversion,
    return_on_ad_spend as existing_return_on_ad_spend,
    
    -- Use enhanced data quality flag from currency normalization
    enhanced_data_quality_flag as data_quality_flag,
    _dbt_loaded_at
  from {{ ref('int_facebook_ads__currency_normalized') }}
),

calculated_metrics as (
  select
    *,
    
    -- Core business metrics with division by zero handling
    case 
      when impressions > 0 then (clicks / cast(impressions as float64)) * 100.0
      else 0.0
    end as click_through_rate,
    
    case 
      when clicks > 0 then spend / cast(clicks as float64)
      else null
    end as cost_per_click,
    
    case 
      when impressions > 0 then (spend / cast(impressions as float64)) * 1000.0
      else null
    end as cost_per_mille,
    
    case 
      when clicks > 0 then (conversions / cast(clicks as float64)) * 100.0
      else 0.0
    end as conversion_rate,
    
    case 
      when spend > 0 then conversion_value / spend
      else null
    end as return_on_ad_spend,
    
    case 
      when conversions > 0 then spend / cast(conversions as float64)
      else null
    end as cost_per_conversion
  from base_data
),

data_quality_enhanced as (
  select
    *,
    
    -- Enhanced data quality validations
    case 
      when data_quality_flag != 'Valid' then data_quality_flag
      when click_through_rate > 100.0 then 'Invalid CTR'
      when clicks > 0 and (spend / cast(clicks as float64)) < 0 then 'Negative CPC'
      when impressions > 0 and ((spend / cast(impressions as float64)) * 1000.0) < 0 then 'Negative CPM'
      when conversion_rate > 100.0 then 'Invalid Conversion Rate'
      when spend > 0 and (conversion_value / spend) < 0 then 'Negative ROAS'
      when conversions > 0 and (spend / cast(conversions as float64)) < 0 then 'Negative Cost Per Conversion'
      when impressions > 0 and clicks > impressions then 'Clicks Exceed Impressions'
      when reach > 0 and impressions > 0 and abs(frequency - (impressions / cast(reach as float64))) > 0.01 then 'Frequency Calculation Error'
      when spend > 0 and cost_per_click is not null and abs(cost_per_click - (spend / nullif(clicks, 0))) > 0.01 then 'CPC Calculation Mismatch'
      when spend > 0 and cost_per_mille is not null and abs(cost_per_mille - ((spend / nullif(impressions, 0)) * 1000)) > 0.01 then 'CPM Calculation Mismatch'
      else 'Valid'
    end as enhanced_data_quality_flag,
    
    -- Metric reasonableness flags
    case 
      when click_through_rate > 50.0 then 'High CTR'
      when cost_per_click > 10.0 then 'High CPC'
      when conversion_rate > 20.0 then 'High Conversion Rate'
      when return_on_ad_spend > 20.0 then 'High ROAS'
      when frequency > 10.0 then 'High Frequency'
      else 'Normal'
    end as metric_alert_flag,
    
    -- Performance tier classification
    case 
      when click_through_rate >= 2.0 and conversion_rate >= 2.0 and return_on_ad_spend >= 3.0 then 'High Performer'
      when click_through_rate >= 1.0 and conversion_rate >= 1.0 and return_on_ad_spend >= 2.0 then 'Good Performer'
      when click_through_rate >= 0.5 and conversion_rate >= 0.5 and return_on_ad_spend >= 1.0 then 'Average Performer'
      when spend > 0 then 'Poor Performer'
      else 'No Spend'
    end as performance_tier
  from calculated_metrics
)

select
  -- Core identifiers
  insights_key,
  date_day,
  account_id,
  account_name,
  campaign_id,
  campaign_name,
  campaign_objective,
  campaign_status,
  ad_id,
  ad_name,
  
  -- Raw metrics
  impressions,
  clicks,
  spend,
  reach,
  frequency,
  conversions,
  conversion_value,
  
  -- Calculated business metrics (our calculations)
  round(click_through_rate, 4) as click_through_rate,
  round(cost_per_click, 4) as cost_per_click,
  round(cost_per_mille, 4) as cost_per_mille,
  round(conversion_rate, 4) as conversion_rate,
  round(return_on_ad_spend, 4) as return_on_ad_spend,
  round(cost_per_conversion, 4) as cost_per_conversion,
  
  -- Currency and exchange rate information
  account_currency,
  exchange_rate,
  exchange_rate_date,
  exchange_rate_source,
  exchange_rate_quality_flag,
  
  -- Original currency amounts
  spend_original_currency,
  conversion_value_original_currency,
  cost_per_click_original_currency,
  cost_per_mille_original_currency,
  cost_per_conversion_original_currency,
  
  -- USD converted amounts
  round(spend_usd, 4) as spend_usd,
  round(conversion_value_usd, 4) as conversion_value_usd,
  round(cost_per_click_usd, 4) as cost_per_click_usd,
  round(cost_per_mille_usd, 4) as cost_per_mille_usd,
  round(cost_per_conversion_usd, 4) as cost_per_conversion_usd,
  
  -- Existing calculated fields from staging (for comparison)
  round(existing_cost_per_click, 4) as staging_cost_per_click,
  round(existing_cost_per_mille, 4) as staging_cost_per_mille,
  round(existing_click_through_rate, 4) as staging_click_through_rate,
  round(existing_cost_per_conversion, 4) as staging_cost_per_conversion,
  round(existing_return_on_ad_spend, 4) as staging_return_on_ad_spend,
  
  -- Data quality and alerting
  enhanced_data_quality_flag,
  metric_alert_flag,
  performance_tier,
  
  -- Metadata
  _dbt_loaded_at
from data_quality_enhanced
where enhanced_data_quality_flag = 'Valid'