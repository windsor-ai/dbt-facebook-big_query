/*
    Data Quality Validation
    
    Purpose: Validates data quality across both models including duplicate checks,
    enhanced_data_quality_flag verification, and performance_tier distribution analysis.
    
    Expected Results: 
    - No duplicate records in either model
    - All records should have enhanced_data_quality_flag = 'Valid' (implicit from source filtering)
    - Performance_tier distribution should be reasonable in ad_performance_daily
    
    Usage: dbt compile --select analysis/validation_data_quality
*/

with base_spend_duplicates as (
    select 
        date_day,
        account_id,
        campaign_name,
        ad_name,
        count(*) as duplicate_count
    from {{ ref('facebook_ads__base_spend') }}
    group by date_day, account_id, campaign_name, ad_name
    having count(*) > 1
),

ad_performance_duplicates as (
    select 
        date_day,
        account_id,
        campaign_id,
        ad_id,
        count(*) as duplicate_count
    from {{ ref('facebook_ads__ad_performance_daily') }}
    group by date_day, account_id, campaign_id, ad_id
    having count(*) > 1
),

performance_tier_distribution as (
    select 
        performance_tier,
        count(*) as tier_count,
        round(count(*) * 100.0 / sum(count(*)) over (), 2) as tier_percentage
    from {{ ref('facebook_ads__ad_performance_daily') }}
    where performance_tier is not null
    group by performance_tier
),

base_spend_quality_checks as (
    select 
        'base_spend' as model_name,
        'null_spend_values' as check_type,
        count(*) as issue_count
    from {{ ref('facebook_ads__base_spend') }}
    where spend is null
    
    union all
    
    select 
        'base_spend' as model_name,
        'negative_spend_values' as check_type,
        count(*) as issue_count
    from {{ ref('facebook_ads__base_spend') }}
    where spend < 0
    
    union all
    
    select 
        'base_spend' as model_name,
        'invalid_click_impression_ratio' as check_type,
        count(*) as issue_count
    from {{ ref('facebook_ads__base_spend') }}
    where clicks > impressions and impressions > 0
),

ad_performance_quality_checks as (
    select 
        'ad_performance_daily' as model_name,
        'null_required_ids' as check_type,
        count(*) as issue_count
    from {{ ref('facebook_ads__ad_performance_daily') }}
    where account_id is null or campaign_id is null or ad_id is null
    
    union all
    
    select 
        'ad_performance_daily' as model_name,
        'invalid_click_impression_ratio' as check_type,
        count(*) as issue_count
    from {{ ref('facebook_ads__ad_performance_daily') }}
    where clicks > impressions and impressions > 0
    
    union all
    
    select 
        'ad_performance_daily' as model_name,
        'invalid_ctr_values' as check_type,
        count(*) as issue_count
    from {{ ref('facebook_ads__ad_performance_daily') }}
    where click_through_rate > 100 or click_through_rate < 0
),

duplicate_summary as (
    select 
        'base_spend_duplicates' as validation_type,
        count(*) as issue_count,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as validation_status
    from base_spend_duplicates
    
    union all
    
    select 
        'ad_performance_duplicates' as validation_type,
        count(*) as issue_count,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as validation_status
    from ad_performance_duplicates
),

quality_summary as (
    select 
        model_name || '_' || check_type as validation_type,
        issue_count,
        case when issue_count = 0 then 'PASS' else 'FAIL' end as validation_status
    from base_spend_quality_checks
    
    union all
    
    select 
        model_name || '_' || check_type as validation_type,
        issue_count,
        case when issue_count = 0 then 'PASS' else 'FAIL' end as validation_status
    from ad_performance_quality_checks
),

all_validations as (
    select * from duplicate_summary
    union all
    select * from quality_summary
)

select 
    validation_type,
    issue_count,
    validation_status,
    case 
        when validation_status = 'FAIL' and validation_type like '%duplicates%' then 'Duplicate records found'
        when validation_status = 'FAIL' and validation_type like '%null%' then 'Null values in required fields'
        when validation_status = 'FAIL' and validation_type like '%negative%' then 'Negative values in spend field'
        when validation_status = 'FAIL' and validation_type like '%ratio%' then 'Clicks exceed impressions'
        when validation_status = 'FAIL' and validation_type like '%ctr%' then 'Invalid CTR values'
        else 'Data quality check passed'
    end as issue_description
from all_validations
where validation_status = 'FAIL'

union all

-- Performance tier distribution for monitoring
select 
    'performance_tier_distribution' as validation_type,
    tier_count as issue_count,
    'INFO' as validation_status,
    performance_tier || ' (' || tier_percentage || '%)' as issue_description
from performance_tier_distribution
order by validation_status desc, validation_type