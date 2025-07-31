{{ config(materialized='view') }}

with insights_base as (
  select 
    -- All original fields from insights staging model
    insights_key,
    date_day,
    account_id,
    account_name,
    account_currency,
    campaign_id,
    campaign_name,
    campaign_objective,
    campaign_status,
    ad_id,
    ad_name,
    impressions,
    clicks,
    spend,
    reach,
    frequency,
    cost_per_mille,
    cost_per_click,
    click_through_rate,
    conversions,
    conversion_value,
    cost_per_conversion,
    return_on_ad_spend,
    data_quality_flag,
    _dbt_loaded_at
  from {{ ref('stg_facebook_ads__insights') }}
),

-- Exchange rates lookup from seed data
exchange_rates_lookup as (
  select 
    date as exchange_rate_date,
    currency_code,
    usd_exchange_rate,
    source as exchange_rate_source
  from {{ ref('exchange_rates') }}
),

-- Get the most recent exchange rate for each currency within our date range
exchange_rates_with_fill as (
  select 
    currency_code,
    exchange_rate_date,
    usd_exchange_rate,
    exchange_rate_source,
    -- Forward fill exchange rates to cover gaps
    last_value(usd_exchange_rate ignore nulls) over (
      partition by currency_code 
      order by exchange_rate_date 
      rows unbounded preceding
    ) as filled_exchange_rate
  from exchange_rates_lookup
),

insights_with_exchange_rates as (
  select 
    i.*,
    
    -- Join with most recent available exchange rate
    coalesce(er.usd_exchange_rate, er.filled_exchange_rate, 1.0) as exchange_rate,
    er.exchange_rate_date,
    er.exchange_rate_source,
    
    -- Exchange rate quality flags
    case 
      when i.account_currency = 'USD' then 'No Conversion Needed'
      when er.usd_exchange_rate is not null then 'Direct Rate Match'
      when er.filled_exchange_rate is not null then 'Forward Filled Rate'
      else 'No Rate Available'
    end as exchange_rate_quality_flag
    
  from insights_base i
  left join exchange_rates_with_fill er
    on upper(trim(i.account_currency)) = upper(trim(er.currency_code))
    and er.exchange_rate_date <= i.date_day
  qualify row_number() over (
    partition by i.insights_key, er.currency_code 
    order by er.exchange_rate_date desc
  ) = 1
),

currency_validated as (
  select
    *,
    
    -- Currency validation
    case 
      when account_currency is null then 'Missing Currency'
      when length(trim(account_currency)) != 3 then 'Invalid Currency Code'
      when upper(account_currency) not in (
        'USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', 'CNY', 'INR', 'BRL', 'MXN',
        'KRW', 'SGD', 'HKD', 'NOK', 'SEK', 'DKK', 'CHF', 'PLN', 'CZK', 'HUF',
        'RON', 'BGN', 'HRK', 'RUB', 'TRY', 'ZAR', 'ILS', 'AED', 'SAR', 'QAR',
        'KWD', 'BHD', 'OMR', 'JOD', 'LBP', 'EGP', 'MAD', 'TND', 'DZD', 'NGN',
        'GHS', 'KES', 'UGX', 'TZS', 'ETB', 'XOF', 'XAF', 'ZMW', 'BWP', 'SZL',
        'LSL', 'NAD', 'MWK', 'MZN', 'AOA', 'CDF', 'RWF', 'BIF', 'DJF', 'ERN',
        'STD', 'CVE', 'GMD', 'GNF', 'LRD', 'SLL', 'SHP', 'MVR', 'SCR', 'MUR',
        'KMF', 'MGA', 'YER', 'AFN', 'PKR', 'LKR', 'BDT', 'BTN', 'NPR', 'MMK',
        'LAK', 'KHR', 'VND', 'THB', 'MYR', 'IDR', 'PHP', 'TWD', 'MNT', 'KZT',
        'UZS', 'KGS', 'TJS', 'TMT', 'AZN', 'GEL', 'AMD', 'MDL', 'UAH', 'BYN',
        'RSD', 'MKD', 'ALL', 'BAM', 'EUR'  -- Adding EUR again to handle duplicates
      ) then 'Unsupported Currency'
      else 'Valid Currency'
    end as currency_validation_flag,
    
    -- Overall data quality flag combining currency and exchange rate validation
    case 
      when account_currency is null then 'Invalid - Missing Currency'
      when length(trim(account_currency)) != 3 then 'Invalid - Bad Currency Code'
      when upper(account_currency) not in (
        'USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', 'CNY', 'INR', 'BRL', 'MXN',
        'KRW', 'SGD', 'HKD', 'NOK', 'SEK', 'DKK', 'CHF', 'PLN', 'CZK', 'HUF',
        'RON', 'BGN', 'HRK', 'RUB', 'TRY', 'ZAR', 'ILS', 'AED', 'SAR', 'QAR',
        'KWD', 'BHD', 'OMR', 'JOD', 'LBP', 'EGP', 'MAD', 'TND', 'DZD', 'NGN',
        'GHS', 'KES', 'UGX', 'TZS', 'ETB', 'XOF', 'XAF', 'ZMW', 'BWP', 'SZL',
        'LSL', 'NAD', 'MWK', 'MZN', 'AOA', 'CDF', 'RWF', 'BIF', 'DJF', 'ERN',
        'STD', 'CVE', 'GMD', 'GNF', 'LRD', 'SLL', 'SHP', 'MVR', 'SCR', 'MUR',
        'KMF', 'MGA', 'YER', 'AFN', 'PKR', 'LKR', 'BDT', 'BTN', 'NPR', 'MMK',
        'LAK', 'KHR', 'VND', 'THB', 'MYR', 'IDR', 'PHP', 'TWD', 'MNT', 'KZT',
        'UZS', 'KGS', 'TJS', 'TMT', 'AZN', 'GEL', 'AMD', 'MDL', 'UAH', 'BYN',
        'RSD', 'MKD', 'ALL', 'BAM', 'EUR'
      ) then 'Invalid - Unsupported Currency'
      when exchange_rate_quality_flag = 'No Rate Available' then 'Invalid - No Exchange Rate'
      else 'Valid'
    end as enhanced_data_quality_flag
  from insights_with_exchange_rates
),

final_model as (
  select
    -- All original fields from insights
    insights_key,
    date_day,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_objective,
    campaign_status,
    ad_id,
    ad_name,
    impressions,
    clicks,
    spend,
    reach,
    frequency,
    cost_per_mille,
    cost_per_click,
    click_through_rate,
    conversions,
    conversion_value,
    cost_per_conversion,
    return_on_ad_spend,
    data_quality_flag,
    _dbt_loaded_at,
    
    -- Currency information
    account_currency,
    currency_validation_flag,
    enhanced_data_quality_flag,
    
    -- Exchange rate metadata
    exchange_rate,
    exchange_rate_date,
    exchange_rate_source,
    exchange_rate_quality_flag,
    
    -- Original currency amounts (for transparency)
    spend as spend_original_currency,
    conversion_value as conversion_value_original_currency,
    cost_per_click as cost_per_click_original_currency,
    cost_per_mille as cost_per_mille_original_currency,
    cost_per_conversion as cost_per_conversion_original_currency,
    
    -- USD converted amounts
    case 
      when account_currency = 'USD' then spend
      when exchange_rate is not null and exchange_rate > 0 then spend / exchange_rate
      else null
    end as spend_usd,
    
    case 
      when account_currency = 'USD' then conversion_value
      when exchange_rate is not null and exchange_rate > 0 then conversion_value / exchange_rate
      else null
    end as conversion_value_usd,
    
    case 
      when account_currency = 'USD' then cost_per_click
      when exchange_rate is not null and exchange_rate > 0 then cost_per_click / exchange_rate
      else null
    end as cost_per_click_usd,
    
    case 
      when account_currency = 'USD' then cost_per_mille
      when exchange_rate is not null and exchange_rate > 0 then cost_per_mille / exchange_rate
      else null
    end as cost_per_mille_usd,
    
    case 
      when account_currency = 'USD' then cost_per_conversion
      when exchange_rate is not null and exchange_rate > 0 then cost_per_conversion / exchange_rate
      else null
    end as cost_per_conversion_usd
    
  from currency_validated
)

select * from final_model
where enhanced_data_quality_flag = 'Valid'