/*
    Key Field Consistency Check
    
    Purpose: Validates that the same campaigns and ads exist in both models,
    and identifies any missing records in either facebook_ads__base_spend 
    or facebook_ads__ad_performance_daily.
    
    Expected Result: No records should be returned (all keys should exist in both models)
    
    Usage: dbt compile --select analysis/validation_key_field_consistency
*/

with base_spend_keys as (
    select distinct
        date_day,
        account_id,
        campaign_name,
        ad_name
    from {{ ref('facebook_ads__base_spend') }}
),

ad_performance_keys as (
    select distinct
        date_day,
        account_id,
        campaign_name,
        ad_name
    from {{ ref('facebook_ads__ad_performance_daily') }}
),

missing_from_ad_performance as (
    select 
        b.date_day,
        b.account_id,
        b.campaign_name,
        b.ad_name,
        'Missing from ad_performance_daily' as issue_type
    from base_spend_keys b
    left join ad_performance_keys a
        on b.date_day = a.date_day
        and b.account_id = a.account_id
        and b.campaign_name = a.campaign_name
        and b.ad_name = a.ad_name
    where a.date_day is null
),

missing_from_base_spend as (
    select 
        a.date_day,
        a.account_id,
        a.campaign_name,
        a.ad_name,
        'Missing from base_spend' as issue_type
    from ad_performance_keys a
    left join base_spend_keys b
        on a.date_day = b.date_day
        and a.account_id = b.account_id
        and a.campaign_name = b.campaign_name
        and a.ad_name = b.ad_name
    where b.date_day is null
),

all_inconsistencies as (
    select * from missing_from_ad_performance
    union all
    select * from missing_from_base_spend
)

select 
    date_day,
    account_id,
    campaign_name,
    ad_name,
    issue_type,
    'FAIL' as validation_status
from all_inconsistencies
order by date_day desc, account_id, campaign_name, ad_name