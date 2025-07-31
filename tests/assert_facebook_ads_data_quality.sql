-- Test data quality across all Facebook Ads models
-- This test ensures that data quality flags are working correctly

with data_quality_check as (
    select 
        'facebook_ads__base_spend' as model_name,
        count(*) as total_records,
        count(case when spend < 0 then 1 end) as negative_spend_records,
        count(case when clicks > impressions and impressions > 0 then 1 end) as invalid_ctr_records,
        count(case when clicks < 0 or impressions < 0 then 1 end) as negative_metric_records
    from {{ ref('facebook_ads__base_spend') }}
    
    union all
    
    select 
        'facebook_ads__ad_performance_daily' as model_name,
        count(*) as total_records,
        count(case when spend < 0 then 1 end) as negative_spend_records,
        count(case when clicks > impressions and impressions > 0 then 1 end) as invalid_ctr_records,
        count(case when clicks < 0 or impressions < 0 then 1 end) as negative_metric_records
    from {{ ref('facebook_ads__ad_performance_daily') }}
    
    union all
    
    select 
        'facebook_ads__campaign_summary' as model_name,
        count(*) as total_records,
        count(case when spend < 0 then 1 end) as negative_spend_records,
        count(case when clicks > impressions and impressions > 0 then 1 end) as invalid_ctr_records,
        count(case when clicks < 0 or impressions < 0 then 1 end) as negative_metric_records
    from {{ ref('facebook_ads__campaign_summary') }}
    
    union all
    
    select 
        'facebook_ads__audience_metrics' as model_name,
        count(*) as total_records,
        0 as negative_spend_records, -- audience model doesn't have spend
        count(case when clicks > impressions and impressions > 0 then 1 end) as invalid_ctr_records,
        count(case when clicks < 0 or impressions < 0 then 1 end) as negative_metric_records
    from {{ ref('facebook_ads__audience_metrics') }}
)

select 
    model_name,
    total_records,
    negative_spend_records,
    invalid_ctr_records,
    negative_metric_records
from data_quality_check
where negative_spend_records > 0
   or invalid_ctr_records > 0  
   or negative_metric_records > 0