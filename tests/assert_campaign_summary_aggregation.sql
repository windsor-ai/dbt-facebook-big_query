-- Test that campaign summary properly aggregates ad level data
-- This ensures aggregation logic is working correctly

with ad_level_totals as (
    select 
        date_day,
        campaign_id,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(spend) as total_spend,
        sum(conversions) as total_conversions,
        sum(conversion_value) as total_conversion_value
    from {{ ref('facebook_ads__ad_performance_daily') }}
    group by date_day, campaign_id
),

campaign_level_totals as (
    select 
        date_day,
        campaign_id,
        impressions,
        clicks,
        spend,
        conversions,
        conversion_value
    from {{ ref('facebook_ads__campaign_summary') }}
),

comparison as (
    select 
        a.date_day,
        a.campaign_id,
        a.total_impressions as ad_level_impressions,
        c.impressions as campaign_level_impressions,
        a.total_clicks as ad_level_clicks,
        c.clicks as campaign_level_clicks,
        a.total_spend as ad_level_spend,
        c.spend as campaign_level_spend,
        abs(a.total_impressions - c.impressions) as impressions_diff,
        abs(a.total_clicks - c.clicks) as clicks_diff,
        abs(a.total_spend - c.spend) as spend_diff
    from ad_level_totals a
    join campaign_level_totals c
        on a.date_day = c.date_day 
        and a.campaign_id = c.campaign_id
)

-- Fail test if there are significant differences (allowing for small rounding differences)
select *
from comparison  
where impressions_diff > 1
   or clicks_diff > 1
   or spend_diff > 0.01