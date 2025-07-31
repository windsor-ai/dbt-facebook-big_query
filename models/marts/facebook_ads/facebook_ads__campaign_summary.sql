{{
    config(
        materialized='table',
        partition_by={'field': 'date_day', 'data_type': 'date'},
        cluster_by=['account_id', 'campaign_id']
    )
}}

/*
    Facebook Ads Campaign Summary Model
    
    This model provides daily campaign level performance metrics for business analysis.
    
    Use Cases:
    - Campaign performance tracking and optimization
    - Cross campaign performance comparison
    - Budget allocation and analysis
    - Campaign level ROI and ROAS monitoring
    - Performance tier distribution analysis
    
    Grain: One record per campaign per day
    Filters: Only validated data with enhanced quality flags
*/

with campaign_daily_performance as (
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
        
        -- Core performance metrics
        impressions,
        clicks,
        spend,
        reach,
        frequency,
        conversions,
        conversion_value,
        
        -- Calculated efficiency metrics
        click_through_rate,
        cost_per_click,
        cost_per_mille,
        conversion_rate,
        return_on_ad_spend,
        cost_per_conversion,
        
        -- Performance classification
        performance_tier,
        metric_alert_flag
    
    from {{ ref('int_facebook_ads__daily_metrics') }}
    where enhanced_data_quality_flag = 'Valid'
),

campaign_aggregations as (
    select
        -- Date and campaign hierarchy
        date_day,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        campaign_objective,
        campaign_status,
        
        -- Aggregated volume metrics
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(spend) as total_spend,
        sum(reach) as total_reach,
        sum(conversions) as total_conversions,
        sum(conversion_value) as total_conversion_value,
        
        -- Weighted average frequency (impressions/reach across all ads)
        case 
            when sum(reach) > 0 then sum(impressions) / cast(sum(reach) as float64)
            else 0.0
        end as avg_frequency,
        
        -- Performance tier distribution
        countif(performance_tier = 'High Performer') as high_performer_ads,
        countif(performance_tier = 'Good Performer') as good_performer_ads,
        countif(performance_tier = 'Average Performer') as average_performer_ads,
        countif(performance_tier = 'Poor Performer') as poor_performer_ads,
        countif(performance_tier = 'No Spend') as no_spend_ads,
        count(*) as total_ads,
        
        -- Alert flags
        countif(metric_alert_flag != 'Normal') as ads_with_alerts,
        
        -- Count of distinct ads for this campaign on this day
        count(distinct ad_id) as active_ads_count
        
    from campaign_daily_performance
    group by 1, 2, 3, 4, 5, 6, 7
),

campaign_calculated_metrics as (
    select
        *,
        
        -- Campaign-level efficiency metrics
        case 
            when total_impressions > 0 then (total_clicks / cast(total_impressions as float64)) * 100.0
            else 0.0
        end as campaign_click_through_rate,
        
        case 
            when total_clicks > 0 then total_spend / cast(total_clicks as float64)
            else null
        end as campaign_cost_per_click,
        
        case 
            when total_impressions > 0 then (total_spend / cast(total_impressions as float64)) * 1000.0
            else null
        end as campaign_cost_per_mille,
        
        case 
            when total_clicks > 0 then (total_conversions / cast(total_clicks as float64)) * 100.0
            else 0.0
        end as campaign_conversion_rate,
        
        case 
            when total_spend > 0 then total_conversion_value / total_spend
            else null
        end as campaign_return_on_ad_spend,
        
        case 
            when total_conversions > 0 then total_spend / cast(total_conversions as float64)
            else null
        end as campaign_cost_per_conversion,
        
        -- Performance distribution percentages
        case 
            when total_ads > 0 then (high_performer_ads / cast(total_ads as float64)) * 100.0
            else 0.0
        end as high_performer_pct,
        
        case 
            when total_ads > 0 then (good_performer_ads / cast(total_ads as float64)) * 100.0
            else 0.0
        end as good_performer_pct,
        
        case 
            when total_ads > 0 then (average_performer_ads / cast(total_ads as float64)) * 100.0
            else 0.0
        end as average_performer_pct,
        
        case 
            when total_ads > 0 then (poor_performer_ads / cast(total_ads as float64)) * 100.0
            else 0.0
        end as poor_performer_pct,
        
        case 
            when total_ads > 0 then (ads_with_alerts / cast(total_ads as float64)) * 100.0
            else 0.0
        end as alert_rate_pct
        
    from campaign_aggregations
),

final_campaign_metrics as (
    select
        *,
        
        -- Campaign-level performance tier based on aggregated metrics
        case 
            when campaign_click_through_rate >= 2.0 and campaign_conversion_rate >= 2.0 and campaign_return_on_ad_spend >= 3.0 then 'High Performer'
            when campaign_click_through_rate >= 1.0 and campaign_conversion_rate >= 1.0 and campaign_return_on_ad_spend >= 2.0 then 'Good Performer'
            when campaign_click_through_rate >= 0.5 and campaign_conversion_rate >= 0.5 and campaign_return_on_ad_spend >= 1.0 then 'Average Performer'
            when total_spend > 0 then 'Poor Performer'
            else 'No Spend'
        end as campaign_performance_tier,
        
        -- Campaign efficiency score (composite metric)
        case 
            when campaign_click_through_rate is not null and campaign_conversion_rate is not null and campaign_return_on_ad_spend is not null
            then round(
                (campaign_click_through_rate * 0.3) + 
                (campaign_conversion_rate * 0.3) + 
                (campaign_return_on_ad_spend * 0.4), 2
            )
            else null
        end as campaign_efficiency_score
        
    from campaign_calculated_metrics
)

select 
    -- Date dimension
    date_day,
    
    -- Campaign hierarchy
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_objective,
    campaign_status,
    
    -- Volume metrics
    total_impressions as impressions,
    total_clicks as clicks,
    total_spend as spend,
    total_reach as reach,
    round(avg_frequency, 4) as frequency,
    total_conversions as conversions,
    total_conversion_value as conversion_value,
    
    -- Campaign efficiency metrics
    round(campaign_click_through_rate, 4) as click_through_rate,
    round(campaign_cost_per_click, 4) as cost_per_click,
    round(campaign_cost_per_mille, 4) as cost_per_mille,
    round(campaign_conversion_rate, 4) as conversion_rate,
    round(campaign_return_on_ad_spend, 4) as return_on_ad_spend,
    round(campaign_cost_per_conversion, 4) as cost_per_conversion,
    
    -- Campaign performance indicators
    campaign_performance_tier as performance_tier,
    campaign_efficiency_score as efficiency_score,
    
    -- Ad composition and distribution
    active_ads_count,
    total_ads,
    high_performer_ads,
    good_performer_ads,
    average_performer_ads,
    poor_performer_ads,
    no_spend_ads,
    
    -- Performance distribution percentages
    round(high_performer_pct, 2) as high_performer_pct,
    round(good_performer_pct, 2) as good_performer_pct,
    round(average_performer_pct, 2) as average_performer_pct,
    round(poor_performer_pct, 2) as poor_performer_pct,
    
    -- Alert monitoring
    ads_with_alerts,
    round(alert_rate_pct, 2) as alert_rate_pct

from final_campaign_metrics