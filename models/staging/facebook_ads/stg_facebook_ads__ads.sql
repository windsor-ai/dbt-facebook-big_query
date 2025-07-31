{{ config(materialized='view') }}

with source_data as (
  select * from {{ source('raw_data', 'facebook_ads_windsor_ads') }}
),

unique_ads as (
  select 
    actor_id,
    adset_id,
    ad_id,
    coalesce(max(ad_name), max(case when ad_name is not null then ad_name end)) as ad_name,
    max(ad_created_time) as ad_created_time,
    coalesce(max(ad_object_type), max(case when ad_object_type is not null then ad_object_type end)) as ad_object_type,
    coalesce(max(status), max(case when status is not null then status end)) as ad_status,
    
    -- Creative information
    coalesce(max(title), max(case when title is not null then title end)) as ad_title,
    coalesce(max(body), max(case when body is not null then body end)) as ad_body,
    coalesce(max(link), max(case when link is not null then link end)) as ad_link,
    coalesce(max(link_url), max(case when link_url is not null then link_url end)) as ad_link_url,
    coalesce(max(thumbnail_url), max(case when thumbnail_url is not null then thumbnail_url end)) as ad_thumbnail_url,
    coalesce(max(facebook_permalink_url), max(case when facebook_permalink_url is not null then facebook_permalink_url end)) as facebook_permalink_url,
    coalesce(max(instagram_permalink_url), max(case when instagram_permalink_url is not null then instagram_permalink_url end)) as instagram_permalink_url,
    coalesce(max(website_destination_url), max(case when website_destination_url is not null then website_destination_url end)) as website_destination_url,
    
    -- Preview URLs
    coalesce(max(desktop_feed_standard_preview_url), max(case when desktop_feed_standard_preview_url is not null then desktop_feed_standard_preview_url end)) as desktop_feed_standard_preview_url,
    coalesce(max(facebook_story_mobile_preview_url), max(case when facebook_story_mobile_preview_url is not null then facebook_story_mobile_preview_url end)) as facebook_story_mobile_preview_url,
    coalesce(max(instagram_standard_preview_url), max(case when instagram_standard_preview_url is not null then instagram_standard_preview_url end)) as instagram_standard_preview_url,
    coalesce(max(instagram_story_preview_url), max(case when instagram_story_preview_url is not null then instagram_story_preview_url end)) as instagram_story_preview_url,
    
    -- Additional metadata
    coalesce(max(url_tags), max(case when url_tags is not null then url_tags end)) as url_tags,
    coalesce(max(source_instagram_media_id), max(case when source_instagram_media_id is not null then source_instagram_media_id end)) as source_instagram_media_id,
    max(instagram_actor_id) as instagram_actor_id
    
  from source_data
  where ad_id is not null
    and ad_id != ''
    and actor_id is not null
    and actor_id != ''
  group by actor_id, adset_id, ad_id
),

cleaned_ads as (
  select
    {{ dbt_utils.generate_surrogate_key(['ad_id']) }} as ad_key,
    {{ dbt_utils.generate_surrogate_key(['adset_id']) }} as adset_key,
    {{ dbt_utils.generate_surrogate_key(['actor_id']) }} as account_key,
    
    cast(ad_id as string) as ad_id,
    cast(adset_id as string) as adset_id,
    cast(actor_id as string) as account_id,
    
    -- Ad name cleaning and standardization
    case 
      when trim(ad_name) = '' or ad_name is null then 'Unknown Ad'
      when regexp_contains(trim(ad_name), r'^[0-9]+$') then concat('Ad ', trim(ad_name))
      else trim(regexp_replace(ad_name, r'\s+', ' '))
    end as ad_name_clean,
    
    cast(ad_name as string) as ad_name_raw,
    
    -- Ad status standardization
    case 
      when upper(trim(coalesce(ad_status, ''))) in ('ACTIVE', 'LEARNING', 'LEARNING LIMITED') then 'ACTIVE'
      when upper(trim(coalesce(ad_status, ''))) = 'PAUSED' then 'PAUSED'
      when upper(trim(coalesce(ad_status, ''))) in ('ARCHIVED', 'DELETED') then 'ARCHIVED'
      when upper(trim(coalesce(ad_status, ''))) = 'SCHEDULED' then 'SCHEDULED'
      when upper(trim(coalesce(ad_status, ''))) in ('UNDER REVIEW', 'PENDING REVIEW', 'IN REVIEW') then 'UNDER_REVIEW'
      when upper(trim(coalesce(ad_status, ''))) in ('REJECTED', 'DISAPPROVED') then 'REJECTED'
      when upper(trim(coalesce(ad_status, ''))) in ('ERROR', 'NO DELIVERY', 'NOT DELIVERING', 'LIMITED DELIVERY') then 'ERROR'
      when upper(trim(coalesce(ad_status, ''))) = 'DRAFT' then 'DRAFT'
      when upper(trim(coalesce(ad_status, ''))) = 'COMPLETED' then 'COMPLETED'
      else 'UNKNOWN'
    end as ad_status_clean,
    
    cast(coalesce(ad_status, 'Unknown') as string) as ad_status_raw,
    
    -- Ad metadata
    cast(ad_object_type as string) as ad_object_type,
    cast(ad_created_time as timestamp) as ad_created_time,
    
    -- Creative information
    case 
      when trim(ad_title) = '' or ad_title is null then null
      else trim(regexp_replace(ad_title, r'\s+', ' '))
    end as ad_title_clean,
    
    cast(ad_title as string) as ad_title_raw,
    
    case 
      when trim(ad_body) = '' or ad_body is null then null
      else trim(regexp_replace(ad_body, r'\s+', ' '))
    end as ad_body_clean,
    
    cast(ad_body as string) as ad_body_raw,
    
    cast(ad_link as string) as ad_link,
    cast(ad_link_url as string) as ad_link_url,
    cast(ad_thumbnail_url as string) as ad_thumbnail_url,
    cast(facebook_permalink_url as string) as facebook_permalink_url,
    cast(instagram_permalink_url as string) as instagram_permalink_url,
    cast(website_destination_url as string) as website_destination_url,
    
    -- Preview URLs
    cast(desktop_feed_standard_preview_url as string) as desktop_feed_standard_preview_url,
    cast(facebook_story_mobile_preview_url as string) as facebook_story_mobile_preview_url,
    cast(instagram_standard_preview_url as string) as instagram_standard_preview_url,
    cast(instagram_story_preview_url as string) as instagram_story_preview_url,
    
    -- Additional metadata
    cast(url_tags as string) as url_tags,
    cast(source_instagram_media_id as string) as source_instagram_media_id,
    cast(instagram_actor_id as string) as instagram_actor_id,
    
    -- Ad quality flags
    case 
      when lower(trim(ad_name)) like '%test%' 
        or lower(trim(ad_name)) like '%demo%'
        or lower(trim(ad_name)) like '%sample%'
        or lower(trim(ad_name)) like '%trial%'
        or regexp_contains(lower(trim(ad_name)), r'\b(test|demo|sample|trial)\b')
      then true
      else false
    end as is_test_ad,
    
    case 
      when ad_name is null or trim(ad_name) = '' then 'Missing Name'
      when regexp_contains(trim(ad_name), r'^[0-9]+$') then 'Numeric Only'
      when length(trim(ad_name)) < 3 then 'Too Short'
      else 'Valid'
    end as ad_name_quality_flag,
    
    -- Creative content quality indicators
    case 
      when ad_title is not null and ad_body is not null then 'Title and Body'
      when ad_title is not null and ad_body is null then 'Title Only'
      when ad_title is null and ad_body is not null then 'Body Only'
      else 'No Title or Body'
    end as creative_content_type,
    
    case 
      when ad_link_url is not null or website_destination_url is not null then true
      else false
    end as has_destination_url,
    
    case 
      when facebook_permalink_url is not null or instagram_permalink_url is not null then true
      else false
    end as has_social_permalink,
    
    case 
      when ad_thumbnail_url is not null then true
      else false
    end as has_thumbnail,
    
    current_timestamp() as _dbt_loaded_at
    
  from unique_ads
)

select * from cleaned_ads
where ad_id is not null
  and ad_id != ''
  and account_id is not null
  and account_id != ''
order by ad_name_clean