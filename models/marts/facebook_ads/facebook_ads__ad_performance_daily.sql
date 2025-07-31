{{
    config(
        materialized='table',
        partition_by={'field': 'date_day', 'data_type': 'date'},
        cluster_by=['account_id', 'campaign_id']
    )
}}

/*
    Facebook Ads Daily Ad Performance Model
    
    This model provides comprehensive daily ad-level performance metrics for business analysis.
    
    Use Cases:
    - Daily performance tracking and optimization
    - Campaign and ad-level attribution analysis
    - Performance benchmarking and trend analysis
    - ROI and efficiency metric monitoring
    - Alert-based performance management
    
    Grain: One record per ad per day
    Filters: Only validated data with enhanced quality flags
*/

with daily_ad_performance as (
    select
        -- Date and identifiers
        date_day,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        campaign_objective,
        campaign_status,
        ad_id,
        ad_name,
        
        -- Core performance metrics
        impressions,
        clicks,
        spend,
        reach,
        frequency,
        conversions,
        conversion_value,
        
        -- Calculated efficiency metrics (pre-validated from intermediate model)
        click_through_rate,
        cost_per_click,
        cost_per_mille,
        conversion_rate,
        return_on_ad_spend,
        cost_per_conversion,
        
        -- Performance classification and alerts
        performance_tier,
        metric_alert_flag
    
    from {{ ref('int_facebook_ads__daily_metrics') }}
    where enhanced_data_quality_flag = 'Valid'
)

select 
    -- Date dimension
    date_day,
    
    -- Account hierarchy
    account_id,
    account_name,
    
    -- Campaign hierarchy  
    campaign_id,
    campaign_name,
    campaign_objective,
    campaign_status,
    
    -- Ad hierarchy
    ad_id,
    ad_name,
    
    -- Volume metrics
    impressions,
    clicks,
    spend,
    reach,
    frequency,
    conversions,
    conversion_value,
    
    -- Efficiency metrics
    click_through_rate,
    cost_per_click,
    cost_per_mille,
    conversion_rate,
    return_on_ad_spend,
    cost_per_conversion,
    
    -- Performance indicators
    performance_tier,
    metric_alert_flag

from daily_ad_performance