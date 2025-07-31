# Facebook Ads Package Macros Documentation

This document provides documentation for all macros included in the Facebook Ads dbt package.

## Overview

The package includes 5 utility macros and 4 custom tests designed specifically for Facebook Ads data processing and validation. These macros follow dbt best practices and are designed to be reusable across different Facebook Ads models.

---

## 🔧 Utility Macros

### 1. `calculate_performance_metrics`

**Purpose**: Calculate standard Facebook Ads performance metrics in a consistent way across models.

**Usage**:
```sql
{{ calculate_performance_metrics('impressions', 'clicks', 'spend', 'conversions', 'conversion_value') }}
```

**Parameters**:
- `impressions_col` (required): Column name for impressions
- `clicks_col` (required): Column name for clicks  
- `spend_col` (required): Column name for spend
- `conversions_col` (optional): Column name for conversions
- `conversion_value_col` (optional): Column name for conversion value

**Returns**: SQL expressions for the following calculated metrics:
- `click_through_rate`: (clicks / impressions) * 100
- `cost_per_click`: spend / clicks
- `cost_per_mille`: (spend / impressions) * 1000

If conversion columns are provided, also returns:
- `conversion_rate`: (conversions / clicks) * 100
- `cost_per_conversion`: spend / conversions
- `return_on_ad_spend`: conversion_value / spend

**Example**:
```sql
select 
    date_day,
    campaign_id,
    impressions,
    clicks,
    spend,
    {{ calculate_performance_metrics('impressions', 'clicks', 'spend') }}
from my_facebook_ads_data
```

---

### 2. `classify_performance_tier`

**Purpose**: Classify ads or campaigns into performance tiers based on key metrics.

**Usage**:
```sql
{{ classify_performance_tier('click_through_rate', 'conversion_rate', 'return_on_ad_spend') }} as performance_tier
```

**Parameters**:
- `ctr_col` (required): Column name for click-through rate
- `conversion_rate_col` (optional): Column name for conversion rate
- `roas_col` (optional): Column name for return on ad spend
- `spend_col` (optional): Column name for spend (default: 'spend')

**Returns**: Performance tier classification:
- `'High Performer'`: CTR ≥ 2.0%, CVR ≥ 2.0%, ROAS ≥ 3.0 (if conversion metrics provided)
- `'Good Performer'`: CTR ≥ 1.0%, CVR ≥ 1.0%, ROAS ≥ 2.0
- `'Average Performer'`: CTR ≥ 0.5%, CVR ≥ 0.5%, ROAS ≥ 1.0
- `'Poor Performer'`: Has spend but doesn't meet above criteria
- `'No Spend'`: No spend recorded

**Example**:
```sql
select 
    ad_id,
    click_through_rate,
    conversion_rate,
    return_on_ad_spend,
    {{ classify_performance_tier('click_through_rate', 'conversion_rate', 'return_on_ad_spend') }} as tier
from facebook_ads__ad_performance_daily
```

---

### 3. `standardize_campaign_objective`

**Purpose**: Standardize Facebook campaign objectives to consistent, readable names.

**Usage**:
```sql
{{ standardize_campaign_objective('campaign_objective') }} as standardized_objective
```

**Parameters**:
- `objective_col` (required): Column name containing campaign objective

**Returns**: Standardized objective names for common Facebook objectives:
- `LEAD_GENERATION` → `'Lead Generation'`
- `CONVERSIONS` → `'Conversions'`
- `TRAFFIC` → `'Traffic'`
- `BRAND_AWARENESS` → `'Brand Awareness'`
- And many more...

**Example**:
```sql
select 
    campaign_id,
    campaign_objective,
    {{ standardize_campaign_objective('campaign_objective') }} as clean_objective
from facebook_ads_data
```

---

### 4. `validate_facebook_ads_data`

**Purpose**: Generate data quality validation flags for Facebook Ads data.

**Usage**:
```sql
{{ validate_facebook_ads_data('date_day', 'account_id', 'campaign_id', 'ad_id', 'impressions', 'clicks', 'spend') }} as data_quality_flag
```

**Parameters**:
- `date_col` (required): Column name for date
- `account_id_col` (required): Column name for account ID
- `campaign_id_col` (required): Column name for campaign ID  
- `ad_id_col` (required): Column name for ad ID
- `impressions_col` (required): Column name for impressions
- `clicks_col` (required): Column name for clicks
- `spend_col` (required): Column name for spend
- `frequency_col` (optional): Column name for frequency

**Returns**: Data quality flags:
- `'Valid'`: Data passes all validation checks
- `'Missing Key Fields'`: Required identifiers are null
- `'Negative Metrics'`: Spend, clicks, or impressions are negative
- `'Invalid CTR'`: Clicks exceed impressions
- `'Invalid Frequency'`: Frequency is negative (if frequency column provided)

**Example**:
```sql
select 
    *,
    {{ validate_facebook_ads_data('date_day', 'account_id', 'campaign_id', 'ad_id', 'impressions', 'clicks', 'spend', 'frequency') }} as quality_flag
from raw_facebook_data
where quality_flag = 'Valid'
```

---

### 5. `generate_facebook_ads_surrogate_key`

**Purpose**: Generate consistent surrogate keys for different Facebook Ads data grains.

**Usage**:
```sql
{{ generate_facebook_ads_surrogate_key('date_day', 'account_id', 'campaign_id', 'ad_id') }} as surrogate_key
```

**Parameters**:
- `date_col` (required): Column name for date
- `account_id_col` (optional): Column name for account ID
- `campaign_id_col` (optional): Column name for campaign ID
- `ad_id_col` (optional): Column name for ad ID
- `audience_type_col` (optional): Audience type column (for audience models)
- `segment_1_col` (optional): First segment column (for audience models)
- `segment_2_col` (optional): Second segment column (for audience models)

**Returns**: Hash-based surrogate key using dbt_utils.generate_surrogate_key()

**Example**:
```sql
-- Ad-level key
select 
    {{ generate_facebook_ads_surrogate_key('date_day', 'account_id', 'campaign_id', 'ad_id') }} as ad_key,
    *
from ad_performance_data

-- Campaign-level key  
select 
    {{ generate_facebook_ads_surrogate_key('date_day', 'account_id', 'campaign_id') }} as campaign_key,
    *
from campaign_performance_data

-- Audience-level key
select 
    {{ generate_facebook_ads_surrogate_key('date_day', audience_type_col='audience_type', segment_1_col='age', segment_2_col='gender') }} as audience_key,
    *
from audience_performance_data
```

---

## 🧪 Custom Tests

### 1. `facebook_ads_ctr_range`

**Purpose**: Test that click-through rate values are within valid range (0-100%).

**Usage**:
```yaml
models:
  - name: my_facebook_model
    columns:
      - name: click_through_rate
        tests:
          - facebook_ads_ctr_range:
              min_value: 0
              max_value: 100
              config:
                where: "impressions > 0"
```

**Parameters**:
- `min_value` (optional): Minimum acceptable CTR (default: 0)
- `max_value` (optional): Maximum acceptable CTR (default: 100)

---

### 2. `facebook_ads_metric_consistency`

**Purpose**: Test that clicks never exceed impressions in Facebook Ads data.

**Usage**:
```yaml
models:
  - name: my_facebook_model
    tests:
      - facebook_ads_metric_consistency:
          impressions_column: 'impressions'
          clicks_column: 'clicks'
          config:
            where: "impressions > 0"
```

**Parameters**:
- `impressions_column` (optional): Column name for impressions (default: 'impressions')
- `clicks_column` (optional): Column name for clicks (default: 'clicks')

---

### 3. `facebook_ads_spend_consistency`

**Purpose**: Test that spend data is consistent with activity (no spend without impressions/clicks).

**Usage**:
```yaml
models:
  - name: my_facebook_model
    tests:
      - facebook_ads_spend_consistency:
          spend_column: 'spend'
          clicks_column: 'clicks'
          impressions_column: 'impressions'
```

**Parameters**:
- `spend_column` (optional): Column name for spend (default: 'spend')
- `clicks_column` (optional): Column name for clicks (default: 'clicks')
- `impressions_column` (optional): Column name for impressions (default: 'impressions')

---

### 4. `facebook_ads_performance_tier_valid`

**Purpose**: Test that performance tier values are from the expected set.

**Usage**:
```yaml
models:
  - name: my_facebook_model
    columns:
      - name: performance_tier
        tests:
          - facebook_ads_performance_tier_valid
```

**Expected Values**:
- 'High Performer'
- 'Good Performer'
- 'Average Performer'  
- 'Poor Performer'
- 'No Spend'

---

## 📋 Usage Guidelines

### Best Practices

1. **Consistent Naming**: Use consistent column names across models to maximize macro reusability
2. **Null Handling**: All macros include proper null handling and safe divisions
3. **Performance**: Use appropriate WHERE clauses in tests to avoid unnecessary computation
4. **Documentation**: Document macro usage in your model files for team clarity

### Common Patterns

```sql
-- Pattern 1: Full performance analysis
select 
    date_day,
    campaign_id,
    ad_id,
    
    -- Raw metrics
    impressions,
    clicks, 
    spend,
    conversions,
    conversion_value,
    
    -- Calculated metrics using macro
    {{ calculate_performance_metrics('impressions', 'clicks', 'spend', 'conversions', 'conversion_value') }},
    
    -- Performance classification
    {{ classify_performance_tier('click_through_rate', 'conversion_rate', 'return_on_ad_spend') }} as performance_tier,
    
    -- Data quality validation
    {{ validate_facebook_ads_data('date_day', 'account_id', 'campaign_id', 'ad_id', 'impressions', 'clicks', 'spend') }} as quality_flag,
    
    -- Unique identifier
    {{ generate_facebook_ads_surrogate_key('date_day', 'account_id', 'campaign_id', 'ad_id') }} as unique_key
    
from raw_facebook_ads_data
where quality_flag = 'Valid'
```

### Integration with Package Models

These macros are already integrated into the package models:
- **facebook_ads__ad_performance_daily**: Uses performance metrics and classification macros
- **facebook_ads__campaign_summary**: Uses aggregated performance metrics and tier classification
- **int_facebook_ads__daily_metrics**: Uses data validation and standardization macros
- **stg_facebook_ads__insights**: Uses objective standardization and data validation

You can use these same macros when extending the package or creating custom models that follow the same patterns.