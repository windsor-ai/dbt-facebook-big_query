{{ config(materialized='table', partition_by={'field': 'date_day', 'data_type': 'date'}) }}

with base_spend as (
    select
        date_day,
        campaign_name,
        ad_name,
        impressions,
        clicks,
        spend,
        conversions,
        campaign_objective as objective,
        account_id,
        click_through_rate,
        cost_per_click,
        cost_per_conversion,
        return_on_ad_spend
    
    from {{ ref('int_facebook_ads__daily_metrics') }}
    where enhanced_data_quality_flag = 'Valid'
)

select 
    date_day as date_day,
    campaign_name as campaign_name,
    ad_name as ad_name,
    impressions as impressions,
    clicks as clicks,
    spend as spend,
    conversions as conversions,
    objective as objective,
    account_id as account_id,
    click_through_rate as click_through_rate,
    cost_per_click as cost_per_click,
    cost_per_conversion as cost_per_conversion,
    return_on_ad_spend as return_on_ad_spend

from base_spend