/*
    Business Logic Validation
    
    Purpose: Validates that calculated metrics follow expected business logic rules
    and relationships across both facebook_ads__base_spend and facebook_ads__ad_performance_daily.
    
    Expected Results: All business logic rules should pass validation
    
    Business Rules Tested:
    1. clicks <= impressions (fundamental constraint)
    2. spend >= 0 (no negative spend)
    3. conversions <= clicks (can't convert without clicking)
    4. CTR calculation validation (clicks/impressions * 100)
    5. CPC calculation validation (spend/clicks)
    6. ROAS reasonableness (not extremely high without justification)
    7. Frequency >= 1 (if reach > 0, frequency should be at least 1)
    
    Usage: dbt compile --select analysis/validation_business_logic
*/

with base_spend_logic_checks as (
    select 
        'base_spend' as model_name,
        date_day,
        account_id,
        campaign_name,
        ad_name,
        
        -- Rule 1: Clicks should not exceed impressions
        case when clicks > impressions and impressions > 0 then 1 else 0 end as clicks_exceed_impressions,
        
        -- Rule 2: Spend should be non-negative
        case when spend < 0 then 1 else 0 end as negative_spend,
        
        -- Rule 3: Conversions should not exceed clicks
        case when conversions > clicks and clicks > 0 then 1 else 0 end as conversions_exceed_clicks,
        
        -- Rule 4: CTR calculation validation (allowing 1% tolerance for rounding)
        case 
            when impressions > 0 and abs(click_through_rate - (clicks * 100.0 / impressions)) > 1.0 
            then 1 else 0 
        end as invalid_ctr_calculation,
        
        -- Rule 5: CPC calculation validation (allowing $0.01 tolerance for rounding)
        case 
            when clicks > 0 and abs(cost_per_click - (spend / clicks)) > 0.01 
            then 1 else 0 
        end as invalid_cpc_calculation,
        
        -- Rule 6: ROAS reasonableness check (flag extremely high ROAS > 50 for review)
        case when return_on_ad_spend > 50 then 1 else 0 end as extremely_high_roas,
        
        -- Rule 7: Cost per conversion should be reasonable relative to spend
        case 
            when conversions > 0 and abs(cost_per_conversion - (spend / conversions)) > 0.01 
            then 1 else 0 
        end as invalid_cpconv_calculation
        
    from {{ ref('facebook_ads__base_spend') }}
),

ad_performance_logic_checks as (
    select 
        'ad_performance_daily' as model_name,
        date_day,
        account_id,
        campaign_name,
        ad_name,
        
        -- Rule 1: Clicks should not exceed impressions
        case when clicks > impressions and impressions > 0 then 1 else 0 end as clicks_exceed_impressions,
        
        -- Rule 2: Spend should be non-negative
        case when spend < 0 then 1 else 0 end as negative_spend,
        
        -- Rule 3: Conversions should not exceed clicks
        case when conversions > clicks and clicks > 0 then 1 else 0 end as conversions_exceed_clicks,
        
        -- Rule 4: CTR should be between 0 and 100
        case when click_through_rate < 0 or click_through_rate > 100 then 1 else 0 end as invalid_ctr_range,
        
        -- Rule 5: Frequency should be >= 1 if reach > 0
        case when reach > 0 and frequency < 1 then 1 else 0 end as invalid_frequency,
        
        -- Rule 6: Reach should not exceed impressions
        case when reach > impressions and impressions > 0 then 1 else 0 end as reach_exceeds_impressions,
        
        -- Rule 7: Cost per mille validation (allowing $0.01 tolerance)
        case 
            when impressions > 0 and abs(cost_per_mille - (spend * 1000.0 / impressions)) > 0.01 
            then 1 else 0 
        end as invalid_cpm_calculation,
        
        -- Rule 8: Conversion rate should be between 0 and 100
        case when conversion_rate < 0 or conversion_rate > 100 then 1 else 0 end as invalid_conversion_rate,
        
        -- Rule 9: Performance tier should have valid values
        case 
            when performance_tier not in ('High Performer', 'Good Performer', 'Average Performer', 'Poor Performer', 'No Spend')
            and performance_tier is not null
            then 1 else 0 
        end as invalid_performance_tier
        
    from {{ ref('facebook_ads__ad_performance_daily') }}
),

base_spend_summary as (
    select 
        model_name,
        'clicks_exceed_impressions' as rule_name,
        sum(clicks_exceed_impressions) as violation_count,
        count(*) as total_records
    from base_spend_logic_checks
    group by model_name
    having sum(clicks_exceed_impressions) > 0
    
    union all
    
    select 
        model_name,
        'negative_spend' as rule_name,
        sum(negative_spend) as violation_count,
        count(*) as total_records
    from base_spend_logic_checks
    group by model_name
    having sum(negative_spend) > 0
    
    union all
    
    select 
        model_name,
        'conversions_exceed_clicks' as rule_name,
        sum(conversions_exceed_clicks) as violation_count,
        count(*) as total_records
    from base_spend_logic_checks
    group by model_name
    having sum(conversions_exceed_clicks) > 0
    
    union all
    
    select 
        model_name,
        'invalid_ctr_calculation' as rule_name,
        sum(invalid_ctr_calculation) as violation_count,
        count(*) as total_records
    from base_spend_logic_checks
    group by model_name
    having sum(invalid_ctr_calculation) > 0
    
    union all
    
    select 
        model_name,
        'invalid_cpc_calculation' as rule_name,
        sum(invalid_cpc_calculation) as violation_count,
        count(*) as total_records
    from base_spend_logic_checks
    group by model_name
    having sum(invalid_cpc_calculation) > 0
    
    union all
    
    select 
        model_name,
        'extremely_high_roas' as rule_name,
        sum(extremely_high_roas) as violation_count,
        count(*) as total_records
    from base_spend_logic_checks
    group by model_name
    having sum(extremely_high_roas) > 0
    
    union all
    
    select 
        model_name,
        'invalid_cpconv_calculation' as rule_name,
        sum(invalid_cpconv_calculation) as violation_count,
        count(*) as total_records
    from base_spend_logic_checks
    group by model_name
    having sum(invalid_cpconv_calculation) > 0
),

ad_performance_summary as (
    select 
        model_name,
        'clicks_exceed_impressions' as rule_name,
        sum(clicks_exceed_impressions) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(clicks_exceed_impressions) > 0
    
    union all
    
    select 
        model_name,
        'negative_spend' as rule_name,
        sum(negative_spend) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(negative_spend) > 0
    
    union all
    
    select 
        model_name,
        'conversions_exceed_clicks' as rule_name,
        sum(conversions_exceed_clicks) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(conversions_exceed_clicks) > 0
    
    union all
    
    select 
        model_name,
        'invalid_ctr_range' as rule_name,
        sum(invalid_ctr_range) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(invalid_ctr_range) > 0
    
    union all
    
    select 
        model_name,
        'invalid_frequency' as rule_name,
        sum(invalid_frequency) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(invalid_frequency) > 0
    
    union all
    
    select 
        model_name,
        'reach_exceeds_impressions' as rule_name,
        sum(reach_exceeds_impressions) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(reach_exceeds_impressions) > 0
    
    union all
    
    select 
        model_name,
        'invalid_cpm_calculation' as rule_name,
        sum(invalid_cpm_calculation) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(invalid_cpm_calculation) > 0
    
    union all
    
    select 
        model_name,
        'invalid_conversion_rate' as rule_name,
        sum(invalid_conversion_rate) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(invalid_conversion_rate) > 0
    
    union all
    
    select 
        model_name,
        'invalid_performance_tier' as rule_name,
        sum(invalid_performance_tier) as violation_count,
        count(*) as total_records
    from ad_performance_logic_checks
    group by model_name
    having sum(invalid_performance_tier) > 0
),

all_violations as (
    select * from base_spend_summary
    union all
    select * from ad_performance_summary
)

select 
    model_name,
    rule_name,
    violation_count,
    total_records,
    round(violation_count * 100.0 / total_records, 2) as violation_percentage,
    'FAIL' as validation_status,
    case 
        when rule_name = 'clicks_exceed_impressions' then 'Clicks exceed impressions - data quality issue'
        when rule_name = 'negative_spend' then 'Negative spend values detected'
        when rule_name = 'conversions_exceed_clicks' then 'Conversions exceed clicks - impossible scenario'
        when rule_name = 'invalid_ctr_calculation' then 'CTR calculation does not match clicks/impressions'
        when rule_name = 'invalid_cpc_calculation' then 'CPC calculation does not match spend/clicks'
        when rule_name = 'extremely_high_roas' then 'Extremely high ROAS values require review'
        when rule_name = 'invalid_cpconv_calculation' then 'Cost per conversion calculation incorrect'
        when rule_name = 'invalid_ctr_range' then 'CTR outside valid 0-100% range'
        when rule_name = 'invalid_frequency' then 'Frequency less than 1 when reach > 0'
        when rule_name = 'reach_exceeds_impressions' then 'Reach exceeds impressions - impossible scenario'
        when rule_name = 'invalid_cpm_calculation' then 'CPM calculation does not match formula'
        when rule_name = 'invalid_conversion_rate' then 'Conversion rate outside valid 0-100% range'
        when rule_name = 'invalid_performance_tier' then 'Performance tier contains invalid values'
        else 'Unknown business logic violation'
    end as issue_description
from all_violations
order by model_name, violation_count desc