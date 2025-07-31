# Facebook Ads Field Mapping Documentation

## Source Data Structure (Windsor.ai)

Windsor.ai Facebook Ads data structure:

### Raw Table Schema
| Windsor.ai Field | Data Type | Sample Values | Notes |
|-----------------|-----------|---------------|-------|
| date | DATE | 2024-01-15 | Report date |
| account_id | STRING | 123456789 | Facebook Ad Account ID |
| account_name | STRING | "Company Ads Account" | Business Manager account name |
| campaign_id | STRING | 987654321 | Facebook Campaign ID |
| campaign_name | STRING | "Q1 Lead Generation" | Campaign name |
| campaign_objective | STRING | "LEAD_GENERATION" | Facebook campaign objective |
| campaign_status | STRING | "ACTIVE" | Campaign status |
| ad_id | STRING | 456789123 | Facebook Ad ID |
| ad_name | STRING | "Video Creative A" | Ad name |
| impressions | INTEGER | 10500 | Number of impressions |
| clicks | INTEGER | 250 | Number of clicks |
| spend | FLOAT | 125.50 | Amount spent in account currency |
| reach | INTEGER | 8500 | Unique users reached |
| frequency | FLOAT | 1.24 | Average impressions per user |
| cpm | FLOAT | 11.95 | Cost per 1000 impressions |
| cpc | FLOAT | 0.50 | Cost per click |
| ctr | FLOAT | 2.38 | Click-through rate (%) |
| conversions | INTEGER | 15 | Total conversions |
| conversion_value | FLOAT | 750.00 | Total conversion value |

### Audience Location Data Schema (`facebook_ads_windsor_audience_location`)
| Windsor.ai Field | Data Type | Sample Values | Notes |
|-----------------|-----------|---------------|-------|
| date | DATE | 2024-01-15 | Report date |
| age | STRING | "25-34" | Age demographic segment |
| gender | STRING | "female" | Gender demographic segment |
| clicks | INTEGER | 150 | Number of clicks for this segment |
| frequency | FLOAT | 2.1 | Average impressions per user |
| impressions | INTEGER | 5000 | Number of impressions for this segment |
| reach | INTEGER | 2380 | Unique users reached in this segment |

### Audience Demographics Data Schema (`facebook_ads_windsor_audience_demographics`)
| Windsor.ai Field | Data Type | Sample Values | Notes |
|-----------------|-----------|---------------|-------|
| date | DATE | 2024-01-15 | Report date |
| country | STRING | "United States" | Country location |
| region | STRING | "California" | Region/state location |
| clicks | INTEGER | 200 | Number of clicks for this location |
| frequency | FLOAT | 1.8 | Average impressions per user |
| impressions | INTEGER | 6500 | Number of impressions for this location |
| reach | INTEGER | 3610 | Unique users reached in this location |

## Target Schema Mapping

### Account Report (`facebook_ads__account_report`)

| Target Field | Data Type | Source Field(s) | Transformation | Business Logic |
|--------------|-----------|-----------------|----------------|----------------|
| **Identifiers** |||||
| date_day | DATE | date | Direct mapping | Report date |
| account_id | STRING | account_id | Direct mapping | Facebook Ad Account ID |
| account_name | STRING | account_name | Direct mapping | Account display name |
| **Core Metrics** |||||
| impressions | INTEGER | impressions | SUM() | Total daily impressions |
| clicks | INTEGER | clicks | SUM() | Total daily clicks |
| spend | FLOAT64 | spend | SUM() | Total daily spend |
| reach | INTEGER | reach | SUM() | Total unique reach |
| **Calculated Metrics** |||||
| cost_per_click | FLOAT64 | - | spend / NULLIF(clicks, 0) | Average CPC |
| click_through_rate | FLOAT64 | - | clicks / NULLIF(impressions, 0) * 100 | CTR percentage |
| cost_per_mille | FLOAT64 | - | spend / NULLIF(impressions, 0) * 1000 | CPM |
| frequency | FLOAT64 | - | impressions / NULLIF(reach, 0) | Avg frequency |
| **Conversion Metrics** |||||
| conversions | INTEGER | conversions | SUM() | Total conversions |
| conversion_value | FLOAT64 | conversion_value | SUM() | Total conversion value |
| cost_per_conversion | FLOAT64 | - | spend / NULLIF(conversions, 0) | Average cost per conversion |
| return_on_ad_spend | FLOAT64 | - | conversion_value / NULLIF(spend, 0) | ROAS ratio |

### Campaign Report (`facebook_ads__campaign_report`)

| Target Field | Data Type | Source Field(s) | Transformation | Business Logic |
|--------------|-----------|-----------------|----------------|----------------|
| **Identifiers** |||||
| date_day | DATE | date | Direct mapping | Report date |
| account_id | STRING | account_id | Direct mapping | Parent account |
| account_name | STRING | account_name | Direct mapping | Account display name |
| campaign_id | STRING | campaign_id | Direct mapping | Facebook Campaign ID |
| campaign_name | STRING | campaign_name | Direct mapping | Campaign display name |
| **Campaign Properties** |||||
| campaign_objective | STRING | campaign_objective | Standardized mapping | Mapped to standard values |
| campaign_status | STRING | campaign_status | Standardized mapping | ACTIVE/PAUSED/ARCHIVED |
| **Metrics** |||||
| [All metrics from Account Report] | | | | Aggregated at campaign level |

### Ad Report (`facebook_ads__ad_report`)

| Target Field | Data Type | Source Field(s) | Transformation | Business Logic |
|--------------|-----------|-----------------|----------------|----------------|
| **Identifiers** |||||
| date_day | DATE | date | Direct mapping | Report date |
| account_id | STRING | account_id | Direct mapping | Parent account |
| campaign_id | STRING | campaign_id | Direct mapping | Parent campaign |
| campaign_name | STRING | campaign_name | Direct mapping | Campaign display name |
| ad_id | STRING | ad_id | Direct mapping | Facebook Ad ID |
| ad_name | STRING | ad_name | Direct mapping | Ad display name |
| **Ad Properties** |||||
| campaign_objective | STRING | campaign_objective | Direct mapping | Inherited from campaign |
| **Metrics** |||||
| [All metrics from Account Report] | | | | At ad granularity (no aggregation) |

### Audience Metrics Report (`facebook_ads__audience_metrics`)

| Target Field | Data Type | Source Field(s) | Transformation | Business Logic |
|--------------|-----------|-----------------|----------------|----------------|
| **Identifiers** |||||
| date_day | DATE | date | Direct mapping | Report date |
| audience_type | STRING | - | 'Demographics' or 'Location' | Indicates data source type |
| primary_segment | STRING | age, country | Direct mapping | Age for demographics, country for location |
| secondary_segment | STRING | gender, region | Direct mapping | Gender for demographics, region for location |
| **Core Metrics** |||||
| impressions | INTEGER | impressions | Direct mapping | Impressions for this audience segment |
| clicks | INTEGER | clicks | Direct mapping | Clicks for this audience segment |
| reach | INTEGER | reach | Direct mapping | Unique reach for this segment |
| frequency | FLOAT64 | frequency | Direct mapping | Frequency for this segment |
| **Calculated Metrics** |||||
| click_through_rate | FLOAT64 | - | clicks / NULLIF(impressions, 0) * 100 | CTR percentage for segment |
| calculated_frequency | FLOAT64 | - | impressions / NULLIF(reach, 0) | Validated frequency calculation |

### Campaign Summary Report (`facebook_ads__campaign_summary`)

| Target Field | Data Type | Source Field(s) | Transformation | Business Logic |
|--------------|-----------|-----------------|----------------|----------------|
| **Identifiers** |||||
| date_day | DATE | date | Direct mapping | Report date |
| account_id | STRING | account_id | Direct mapping | Facebook Ad Account ID |
| account_name | STRING | account_name | Direct mapping | Account display name |
| campaign_id | STRING | campaign_id | Direct mapping | Facebook Campaign ID |
| campaign_name | STRING | campaign_name | Direct mapping | Campaign display name |
| campaign_objective | STRING | campaign_objective | Standardized mapping | Mapped to standard values |
| campaign_status | STRING | campaign_status | Direct mapping | Campaign status |
| **Aggregated Metrics** |||||
| impressions | INTEGER | impressions | SUM() | Total campaign impressions (aggregated across all ads) |
| clicks | INTEGER | clicks | SUM() | Total campaign clicks (aggregated across all ads) |
| spend | FLOAT64 | spend | SUM() | Total campaign spend (aggregated across all ads) |
| reach | INTEGER | reach | SUM() | Total campaign reach (aggregated across all ads) |
| conversions | INTEGER | conversions | SUM() | Total campaign conversions (aggregated across all ads) |
| conversion_value | FLOAT64 | conversion_value | SUM() | Total campaign conversion value (aggregated across all ads) |
| **Campaign-Level Calculated Metrics** |||||
| frequency | FLOAT64 | - | total_impressions / NULLIF(total_reach, 0) | Campaign-wide frequency |
| click_through_rate | FLOAT64 | - | total_clicks / NULLIF(total_impressions, 0) * 100 | Campaign-level CTR percentage |
| cost_per_click | FLOAT64 | - | total_spend / NULLIF(total_clicks, 0) | Campaign-level average CPC |
| cost_per_mille | FLOAT64 | - | total_spend / NULLIF(total_impressions, 0) * 1000 | Campaign-level CPM |
| conversion_rate | FLOAT64 | - | total_conversions / NULLIF(total_clicks, 0) * 100 | Campaign-level conversion rate |
| return_on_ad_spend | FLOAT64 | - | total_conversion_value / NULLIF(total_spend, 0) | Campaign-level ROAS |
| cost_per_conversion | FLOAT64 | - | total_spend / NULLIF(total_conversions, 0) | Campaign-level cost per conversion |
| **Performance Analysis** |||||
| performance_tier | STRING | - | Calculated based on efficiency metrics | Campaign performance classification |
| efficiency_score | FLOAT64 | - | Composite metric (CTR*0.3 + CVR*0.3 + ROAS*0.4) | Campaign efficiency score |
| active_ads_count | INTEGER | - | COUNT(DISTINCT ad_id) | Number of distinct active ads in campaign |
| total_ads | INTEGER | - | COUNT(*) | Total ads with activity in campaign |
| high_performer_ads | INTEGER | - | COUNT(*) WHERE performance_tier = 'High Performer' | Count of high performing ads |
| **Distribution Metrics** |||||
| high_performer_pct | FLOAT64 | - | high_performer_ads / total_ads * 100 | Percentage of high performing ads |
| alert_rate_pct | FLOAT64 | - | ads_with_alerts / total_ads * 100 | Percentage of ads with performance alerts |

## Data Transformation Rules

### Campaign Objective Standardization
```sql
CASE 
  WHEN UPPER(campaign_objective) = 'LEAD_GENERATION' THEN 'Lead Generation'
  WHEN UPPER(campaign_objective) = 'CONVERSIONS' THEN 'Conversions'
  WHEN UPPER(campaign_objective) = 'TRAFFIC' THEN 'Traffic'
  WHEN UPPER(campaign_objective) = 'BRAND_AWARENESS' THEN 'Brand Awareness'
  WHEN UPPER(campaign_objective) = 'REACH' THEN 'Reach'
  WHEN UPPER(campaign_objective) = 'VIDEO_VIEWS' THEN 'Video Views'
  WHEN UPPER(campaign_objective) = 'MESSAGES' THEN 'Messages'
  ELSE campaign_objective
END as campaign_objective