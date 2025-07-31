/*
    Metric Consistency Validation
    
    Purpose: Validates that key calculated metrics (click_through_rate, cost_per_click,
    return_on_ad_spend) are identical between facebook_ads__base_spend and
    facebook_ads__ad_performance_daily since both source from the same intermediate model.
    
    Expected Result: All metric differences should be 0 or very close to 0 (accounting for rounding)
    
    Usage: dbt compile --select analysis/validation_metric_consistency
*/

with base_spend_metrics as (
    select 
        date_day,
        account_id,
        campaign_name,
        ad_name,
        click_through_rate as bs_click_through_rate,
        cost_per_click as bs_cost_per_click,
        return_on_ad_spend as bs_return_on_ad_spend,
        cost_per_conversion as bs_cost_per_conversion
    from {{ ref('facebook_ads__base_spend') }}
),

ad_performance_metrics as (
    select 
        date_day,
        account_id,
        campaign_name,
        ad_name,
        click_through_rate as ap_click_through_rate,
        cost_per_click as ap_cost_per_click,
        return_on_ad_spend as ap_return_on_ad_spend,
        cost_per_conversion as ap_cost_per_conversion
    from {{ ref('facebook_ads__ad_performance_daily') }}
),

metric_comparison as (
    select 
        b.date_day,
        b.account_id,
        b.campaign_name,
        b.ad_name,
        
        -- Click Through Rate comparison
        b.bs_click_through_rate,
        a.ap_click_through_rate,
        round(coalesce(b.bs_click_through_rate, 0) - coalesce(a.ap_click_through_rate, 0), 4) as ctr_difference,
        
        -- Cost Per Click comparison
        b.bs_cost_per_click,
        a.ap_cost_per_click,
        round(coalesce(b.bs_cost_per_click, 0) - coalesce(a.ap_cost_per_click, 0), 4) as cpc_difference,
        
        -- Return on Ad Spend comparison
        b.bs_return_on_ad_spend,
        a.ap_return_on_ad_spend,
        round(coalesce(b.bs_return_on_ad_spend, 0) - coalesce(a.ap_return_on_ad_spend, 0), 4) as roas_difference,
        
        -- Cost Per Conversion comparison
        b.bs_cost_per_conversion,
        a.ap_cost_per_conversion,
        round(coalesce(b.bs_cost_per_conversion, 0) - coalesce(a.ap_cost_per_conversion, 0), 4) as cpconv_difference
        
    from base_spend_metrics b
    inner join ad_performance_metrics a
        on b.date_day = a.date_day
        and b.account_id = a.account_id
        and b.campaign_name = a.campaign_name
        and b.ad_name = a.ad_name
),

validation_results as (
    select 
        *,
        case 
            when abs(ctr_difference) > 0.01 
                or abs(cpc_difference) > 0.01 
                or abs(roas_difference) > 0.01 
                or abs(cpconv_difference) > 0.01 
            then 'FAIL'
            else 'PASS'
        end as validation_status,
        
        case 
            when abs(ctr_difference) > 0.01 then 'CTR mismatch '
            else ''
        end ||
        case 
            when abs(cpc_difference) > 0.01 then 'CPC mismatch '
            else ''
        end ||
        case 
            when abs(roas_difference) > 0.01 then 'ROAS mismatch '
            else ''
        end ||
        case 
            when abs(cpconv_difference) > 0.01 then 'CPConv mismatch '
            else ''
        end as issue_description
        
    from metric_comparison
)

select 
    date_day,
    account_id,
    campaign_name,
    ad_name,
    bs_click_through_rate,
    ap_click_through_rate,
    ctr_difference,
    bs_cost_per_click,
    ap_cost_per_click,
    cpc_difference,
    bs_return_on_ad_spend,
    ap_return_on_ad_spend,
    roas_difference,
    bs_cost_per_conversion,
    ap_cost_per_conversion,
    cpconv_difference,
    validation_status,
    trim(issue_description) as issue_description
from validation_results
where validation_status = 'FAIL'
order by date_day desc, account_id, campaign_name, ad_name