{{
    config(
        materialized='table',
        partition_by={'field': 'date_day', 'data_type': 'date'},
        cluster_by=['audience_type', 'date_day']
    )
}}

/*
    Facebook Ads Audience Metrics Model
    
    This model combines audience demographics and location data to provide 
    audience performance insights.
    
    Use Cases:
    - Audience segmentation analysis
    - Demographics and location performance comparison
    - Audience targeting optimization
    - Reach and frequency analysis by audience segments
    
    Grain: One record per audience segment per day
*/

with audience_demographics as (
    select
        cast(date as date) as date_day,
        'Demographics' as audience_type,
        cast(coalesce(age, '') as string) as segment_1,
        cast(coalesce(gender, '') as string) as segment_2,
        null as segment_3,
        
        cast(coalesce(clicks, 0) as int64) as clicks,
        cast(coalesce(frequency, 0.0) as float64) as frequency,
        cast(coalesce(impressions, 0) as int64) as impressions,
        cast(coalesce(reach, 0) as int64) as reach
    
    from {{ source('raw_data', 'facebook_ads_windsor_audience_demographics') }}
    where date is not null
      and date >= '{{ var("facebook_ads_start_date") }}'
),

audience_location as (
    select
        cast(date as date) as date_day,
        'Location' as audience_type,
        cast(coalesce(country, '') as string) as segment_1,
        cast(coalesce(region, '') as string) as segment_2,
        null as segment_3,
        
        cast(coalesce(clicks, 0) as int64) as clicks,
        cast(coalesce(frequency, 0.0) as float64) as frequency,
        cast(coalesce(impressions, 0) as int64) as impressions,
        cast(coalesce(reach, 0) as int64) as reach
    
    from {{ source('raw_data', 'facebook_ads_windsor_audience_location') }}
    where date is not null
      and date >= '{{ var("facebook_ads_start_date") }}'
),

combined_audience_data as (
    select * from audience_demographics
    union all
    select * from audience_location
),

final_metrics as (
    select
        date_day,
        audience_type,
        segment_1,
        segment_2,
        segment_3,
        
        -- Core metrics
        impressions,
        clicks,
        reach,
        frequency,
        
        -- Calculated metrics
        case 
            when impressions > 0 then (clicks / cast(impressions as float64)) * 100.0
            else 0.0
        end as click_through_rate,
        
        case 
            when reach > 0 then impressions / cast(reach as float64)
            else frequency
        end as calculated_frequency,
        
        -- Data quality flags
        case 
            when impressions < 0 or clicks < 0 or reach < 0 then 'Negative Metrics'
            when clicks > impressions and impressions > 0 then 'Invalid CTR'
            when frequency < 0 then 'Invalid Frequency'
            when segment_1 = '' and segment_2 = '' then 'Missing Segments'
            else 'Valid'
        end as data_quality_flag,
        
        current_timestamp() as _dbt_loaded_at
        
    from combined_audience_data
)

select 
    date_day,
    audience_type,
    case 
        when audience_type = 'Demographics' then segment_1 
        when audience_type = 'Location' then segment_1
        else segment_1
    end as primary_segment,
    case 
        when audience_type = 'Demographics' then segment_2
        when audience_type = 'Location' then segment_2
        else segment_2
    end as secondary_segment,
    
    -- Performance metrics
    impressions,
    clicks,
    reach,
    round(frequency, 4) as frequency,
    round(click_through_rate, 4) as click_through_rate,
    round(calculated_frequency, 4) as calculated_frequency,
    
    -- Data quality
    data_quality_flag,
    _dbt_loaded_at

from final_metrics
where data_quality_flag = 'Valid'
  and impressions >= {{ var("min_impressions_threshold", 1) }}
  and (segment_1 != '' or segment_2 != '') 