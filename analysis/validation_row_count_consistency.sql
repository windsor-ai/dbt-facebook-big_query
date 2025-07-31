/*
Row Count Consistency Check

Purpose: Validates that facebook_ads__base_spend and facebook_ads__ad_performance_daily
have identical row counts since both models have the same grain (daily ad-level).

Expected Result: row_count_difference should be 0

Usage: dbt compile --select analysis/validation_row_count_consistency
*/

with base_spend_counts as (
    select 
        date_day,
        count(*) as base_spend_row_count
    from {{ ref('facebook_ads__base_spend') }}
    group by date_day
),

ad_performance_counts as (
    select 
        date_day,
        count(*) as ad_performance_row_count
    from {{ ref('facebook_ads__ad_performance_daily') }}
    group by date_day
),

consistency_check as (
    select 
        coalesce(b.date_day, a.date_day) as date_day,
        coalesce(b.base_spend_row_count, 0) as base_spend_row_count,
        coalesce(a.ad_performance_row_count, 0) as ad_performance_row_count,
        coalesce(b.base_spend_row_count, 0) - coalesce(a.ad_performance_row_count, 0) as row_count_difference
    from base_spend_counts b
    full outer join ad_performance_counts a
        on b.date_day = a.date_day
)

select 
    date_day,
    base_spend_row_count,
    ad_performance_row_count,
    row_count_difference,
    case 
        when row_count_difference = 0 then 'PASS'
        else 'FAIL'
    end as validation_status
from consistency_check
order by date_day desc