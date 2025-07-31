/*
    Spend Totals Validation
    
    Purpose: Validates that total spend amounts match between facebook_ads__base_spend
    and facebook_ads__ad_performance_daily when aggregated by date and account.
    
    Expected Result: spend_difference should be 0 for all records
    
    Usage: dbt compile --select analysis/validation_spend_totals
*/

with base_spend_totals as (
    select 
        date_day,
        account_id,
        sum(spend) as base_spend_total,
        count(*) as base_spend_records
    from {{ ref('facebook_ads__base_spend') }}
    group by date_day, account_id
),

ad_performance_totals as (
    select 
        date_day,
        account_id,
        sum(spend) as ad_performance_total,
        count(*) as ad_performance_records
    from {{ ref('facebook_ads__ad_performance_daily') }}
    group by date_day, account_id
),

spend_comparison as (
    select 
        coalesce(b.date_day, a.date_day) as date_day,
        coalesce(b.account_id, a.account_id) as account_id,
        coalesce(b.base_spend_total, 0) as base_spend_total,
        coalesce(a.ad_performance_total, 0) as ad_performance_total,
        coalesce(b.base_spend_records, 0) as base_spend_records,
        coalesce(a.ad_performance_records, 0) as ad_performance_records,
        round(coalesce(b.base_spend_total, 0) - coalesce(a.ad_performance_total, 0), 2) as spend_difference
    from base_spend_totals b
    full outer join ad_performance_totals a
        on b.date_day = a.date_day 
        and b.account_id = a.account_id
)

select 
    date_day,
    account_id,
    base_spend_total,
    ad_performance_total,
    spend_difference,
    base_spend_records,
    ad_performance_records,
    case 
        when abs(spend_difference) < 0.01 then 'PASS'
        else 'FAIL'
    end as validation_status,
    case 
        when abs(spend_difference) >= 0.01 then 'Spend totals do not match'
        when base_spend_records != ad_performance_records then 'Record counts differ'
        else 'OK'
    end as issue_description
from spend_comparison
where abs(spend_difference) >= 0.01 
   or base_spend_records != ad_performance_records
order by date_day desc, account_id