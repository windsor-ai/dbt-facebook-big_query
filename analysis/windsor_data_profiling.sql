-- Windsor.ai Facebook Ads Data Profiling
{{ config(materialized='view') }}

-- First, understand the table structure
SELECT 
  column_name,
  data_type,
  is_nullable,
  description
FROM `{{ var('facebook_ads_source_table') | replace('.', '.INFORMATION_SCHEMA.COLUMNS') }}`
WHERE table_name = SPLIT('{{ var('facebook_ads_source_table') }}', '.')[OFFSET(2)]
ORDER BY ordinal_position;

-- Analyze data coverage and volumes
WITH data_summary AS (
  SELECT 
    date,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_objective,
    ad_id,
    ad_name,
    impressions,
    clicks,
    spend,
    reach,
    frequency,
    cpm,
    cpc,
    ctr,
    conversions,
    conversion_value,
    CURRENT_TIMESTAMP() as analysis_timestamp
  FROM `{{ var('facebook_ads_source_table') }}`
  WHERE date >= '{{ var('facebook_ads_start_date') }}'
)

SELECT 
  'Data Coverage Analysis' as metric_type,
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  COUNT(*) as total_rows,
  COUNT(DISTINCT account_id) as unique_accounts,
  COUNT(DISTINCT campaign_id) as unique_campaigns,
  COUNT(DISTINCT ad_id) as unique_ads,
  SUM(impressions) as total_impressions,
  SUM(clicks) as total_clicks,
  SUM(spend) as total_spend
FROM data_summary

UNION ALL

-- Account level summary
SELECT 
  CONCAT('Account: ', account_name) as metric_type,
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  COUNT(*) as total_rows,
  COUNT(DISTINCT campaign_id) as unique_campaigns,
  COUNT(DISTINCT ad_id) as unique_ads,
  SUM(impressions) as total_impressions,
  SUM(clicks) as total_clicks,
  SUM(spend) as total_spend
FROM data_summary
GROUP BY account_id, account_name
ORDER BY total_spend DESC

-- Identify data quality issues
WITH quality_check AS (
  SELECT 
    account_id,
    date,
    -- Check for required fields
    CASE WHEN account_id IS NULL THEN 1 ELSE 0 END as missing_account_id,
    CASE WHEN campaign_id IS NULL THEN 1 ELSE 0 END as missing_campaign_id,
    CASE WHEN date IS NULL THEN 1 ELSE 0 END as missing_date,
    
    -- Check for logical inconsistencies
    CASE WHEN clicks > impressions THEN 1 ELSE 0 END as clicks_gt_impressions,
    CASE WHEN spend < 0 THEN 1 ELSE 0 END as negative_spend,
    CASE WHEN impressions < 0 THEN 1 ELSE 0 END as negative_impressions,
    
    -- Check for potential test data
    CASE WHEN LOWER(campaign_name) LIKE '%test%' THEN 1 ELSE 0 END as test_campaign,
    CASE WHEN LOWER(ad_name) LIKE '%test%' THEN 1 ELSE 0 END as test_ad
    
  FROM `{{ var('facebook_ads_source_table') }}`
  WHERE date >= '{{ var('facebook_ads_start_date') }}'
)

SELECT 
  'Quality Issues Summary' as check_type,
  SUM(missing_account_id) as missing_account_ids,
  SUM(missing_campaign_id) as missing_campaign_ids,
  SUM(missing_date) as missing_dates,
  SUM(clicks_gt_impressions) as logical_errors,
  SUM(negative_spend) as negative_spends,
  SUM(negative_impressions) as negative_impressions,
  SUM(test_campaign) as test_campaigns,
  SUM(test_ad) as test_ads,
  COUNT(*) as total_rows_checked
FROM quality_check


-- Understand campaign objectives and types
SELECT 
  campaign_objective,
  COUNT(*) as row_count,
  COUNT(DISTINCT campaign_id) as unique_campaigns,
  COUNT(DISTINCT account_id) as unique_accounts,
  SUM(spend) as total_spend,
  AVG(ctr) as avg_ctr,
  AVG(cpc) as avg_cpc
FROM `{{ var('facebook_ads_source_table') }}`
WHERE date >= '{{ var('facebook_ads_start_date') }}'
  AND campaign_objective IS NOT NULL
GROUP BY campaign_objective
ORDER BY total_spend DESC
