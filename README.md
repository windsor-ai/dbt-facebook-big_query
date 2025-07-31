# dbt Facebook Ads Windsor Package

A production ready dbt package that transforms raw Facebook Ads data from [Windsor.ai](https://windsor.ai/) into clean, analytics ready tables in BigQuery following standardized architecture patterns. You can find a complete list of [available Facebook Ads fields here](https://windsor.ai/data-field/facebook/).

## 🚀 Features
- **Multi-Source Integration**: Support for campaigns, ads, insights, and audience data tables
- **Audience Analytics**: Demographics and location-based audience performance analysis
- **Reusable Macros**: 5 utility macros for consistent metric calculations and data processing
- **Custom Tests**: 4 Facebook Ads-specific tests for data quality validation
- **Data Quality**: Testing suite with deduplication and validation
- **Type Safety**: String to numeric conversions with safe_cast
- **Business Metrics**: Precalculated CTR, CPC, ROAS, and conversion rates
- **Performance Optimized**: BigQuery optimized data types and filtering
- **Windsor.ai Integration**: Purpose built for Windsor.ai Facebook Ads data structure
- **Currency Normalization**: Multi currency support with exchange rate handling
- **Enhanced Metrics**: Performance tiers and alert flags for optimization
- **Data Validation**: Validation queries for data consistency

## 📊 Model Architecture

### Staging Models
| Model | Source Table | Grain | Description |
|-------|--------------|-------|-------------|
| `stg_facebook_ads__campaigns` | `facebook_ads_windsor_campaigns` | Campaign | Campaign level entities with hierarchy and metadata |
| `stg_facebook_ads__ads` | `facebook_ads_windsor_ads` | Ad | Ad level entities with creative information |
| `stg_facebook_ads__insights` | `facebook_ads_windsor_insights` | Date + Account + Campaign + Ad | Daily performance metrics with deduplication |

### Intermediate Models
| Model | Grain | Description |
|-------|-------|-------------|
| `int_facebook_ads__currency_normalized` | Date + Account + Campaign + Ad | Currency conversion and normalization layer |
| `int_facebook_ads__daily_metrics` | Date + Account + Campaign + Ad | Enhanced metrics with performance tiers and quality flags |

### Mart Models
| Model | Grain | Description |
|-------|-------|-------------|
| `facebook_ads__base_spend` | Date + Account + Campaign + Ad | Essential spend tracking with core performance metrics for ROI analysis |
| `facebook_ads__ad_performance_daily` | Date + Account + Campaign + Ad | Performance metrics with clustering for detailed analysis |
| `facebook_ads__campaign_summary` | Date + Campaign | Campaign-level aggregated performance metrics with ad distribution insights |
| `facebook_ads__audience_metrics` | Date + Audience Type + Segment | Audience performance analytics combining demographics and location data |

## 🏗️ Project Structure

```
models/
├── staging/
│   └── facebook_ads/
│       ├── stg_facebook_ads__campaigns.sql  # Campaign entities
│       ├── stg_facebook_ads__ads.sql        # Ad creative entities  
│       ├── stg_facebook_ads__insights.sql   # Performance insights
│       ├── sources.yml                      # Source table definitions
│       └── schema.yml                       # Model documentation & tests
├── intermediate/
│   └── facebook_ads/
│       ├── int_facebook_ads__currency_normalized.sql  # Currency conversion
│       ├── int_facebook_ads__daily_metrics.sql       # Enhanced metrics
│       └── schema.yml                                 # Model documentation & tests
├── marts/
│   └── facebook_ads/
│       ├── facebook_ads__base_spend.sql               # Core spend tracking
│       ├── facebook_ads__ad_performance_daily.sql     # Full performance suite
│       ├── facebook_ads__campaign_summary.sql         # Campaign aggregated metrics
│       ├── facebook_ads__audience_metrics.sql         # Audience analytics
│       └── schema.yml                                 # Model documentation & tests
analysis/
├── docs/
│   ├── package_capabilities.md             # Package capabilities and features documentation
│   ├── field_mapping.md                    # Field mapping reference
│   └── macros_documentation.md             # Macros and custom tests documentation
├── validation_row_count_consistency.sql    # Row count validation
├── validation_spend_totals.sql             # Spend totals validation
├── validation_key_field_consistency.sql    # Key field validation
├── validation_metric_consistency.sql       # Metric consistency validation
├── validation_data_quality.sql             # Data quality validation
├── validation_business_logic.sql           # Business logic validation
└── windsor_data_profiling.sql              # Data profiling queries
data/
└── exchange_rates.csv                      # Sample exchange rate data
tests/
├── assert_facebook_ads_data_quality.sql    # Data quality test
└── assert_campaign_summary_aggregation.sql # Campaign aggregation validation
```

## 🛠 Quick Start

1. **Configure your `dbt_project.yml`**:
```yaml
vars:
  facebook_ads_source_table: 'your-project.raw_data.facebook_ads_windsor_campaigns'
  facebook_ads_start_date: '2024-01-01'
  exclude_test_campaigns: true
  min_spend_threshold: 0
  min_impressions_threshold: 1
```

2. **Source Tables Required**:
- `facebook_ads_windsor_campaigns`: Campaign level data
- `facebook_ads_windsor_ads`: Ad creative data  
- `facebook_ads_windsor_insights`: Performance metrics data
- `facebook_ads_windsor_audience_location`: Audience location performance data (age, gender segments)
- `facebook_ads_windsor_audience_demographics`: Audience demographics performance data (country, region segments)

3. **Run the models**:
```bash
# Run all models
dbt run

# Run specific layers
dbt run --select +stg_facebook_ads    # Staging only
dbt run --select +int_facebook_ads    # Staging + Intermediate
dbt run --select +facebook_ads        # All models

# Run tests
dbt test
```

## 📋 Data Sources

### Source: `facebook_ads_windsor_campaigns`
Contains campaign-level information including objectives, budgets, and status.

**Key Fields**: `account_id`, `campaign_id`, `campaign_name`, `campaign_objective`, `campaign_status`, `campaign_budget_*`

### Source: `facebook_ads_windsor_ads` 
Contains ad-level creative information and metadata.

**Key Fields**: `actor_id`, `ad_id`, `ad_name`, `adset_id`, `title`, `body`, `link_url`, `thumbnail_url`

### Source: `facebook_ads_windsor_insights`
Contains daily performance metrics at the ad level.

**Key Fields**: `date`, `account_id`, `campaign_id`, `ad_id`, `impressions`, `clicks`, `spend`, `actions_purchase`

### Source: `facebook_ads_windsor_audience_location`
Contains audience performance metrics segmented by demographics (age and gender).

**Key Fields**: `date`, `age`, `gender`, `clicks`, `frequency`, `impressions`, `reach`

### Source: `facebook_ads_windsor_audience_demographics`
Contains audience performance metrics segmented by location (country and region).

**Key Fields**: `date`, `country`, `region`, `clicks`, `frequency`, `impressions`, `reach`

## ⚙️ Configuration

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `facebook_ads_start_date` | `2024-01-01` | Start date for data processing |
| `exclude_test_campaigns` | `true` | Filter out test campaigns/ads |
| `min_spend_threshold` | `0` | Minimum spend to include records |
| `min_impressions_threshold` | `1` | Minimum impressions to include records |

### Data Quality Features

- **Deduplication**: Removal of duplicate records by grain
- **Type Conversion**: Safe string to numeric casting for purchase fields
- **Null Handling**: Coalesce logic for missing data
- **Test Coverage**: dbt tests for data validation

## 🧪 Testing & Validation

The package includes data quality tests and validation queries:

### Built-in dbt Tests
- **Uniqueness**: Ensures grain uniqueness across models
- **Not Null**: Validates required fields
- **Referential Integrity**: Checks hierarchical relationships
- **Business Logic**: Validates calculated metrics (ROAS, CPC, etc.)
- **Data Quality**: Flags invalid or suspicious data

### Validation Queries
Located in `analysis/` for ongoing data consistency monitoring:

- **Row Count Consistency**: Validates identical record counts between mart models
- **Spend Totals Validation**: Ensures spend amounts match when aggregated
- **Key Field Consistency**: Checks for missing records between models
- **Metric Consistency**: Validates calculated metrics are identical
- **Data Quality Validation**: Checks for duplicates and invalid values
- **Business Logic Validation**: Validates business rules and metric calculations

Run tests and validations:
```bash
# Run all tests
dbt test

# Run validation queries
dbt compile --select analysis/validation_*
# Then execute the compiled SQL in your BigQuery console
```

## 📈 Calculated Metrics

### Performance Metrics
- **Click-Through Rate**: `clicks / impressions * 100`
- **Cost Per Click**: `spend / clicks`
- **Cost Per Mille**: `spend / impressions * 1000`

### Conversion Metrics  
- **Cost Per Conversion**: `spend / conversions`
- **Return on Ad Spend**: `conversion_value / spend`
- **Conversion Rate**: `conversions / clicks * 100`

## 🔧 Troubleshooting

### Common Issues

**String Conversion Errors**: The package uses `safe_cast()` to handle string to numeric conversions for fields like `actions_purchase`, `action_values_purchase`, and `ctr`.

**Duplicate Records**: The insights model includes deduplication logic that keeps the record with highest spend/impressions for each grain.

**Test Failures**: Check the `return_on_ad_spend_consistency` test it handles cases where conversion data may not be available.

## 🔧 Macros & Custom Tests

This package includes **5 utility macros** and **4 custom tests** designed specifically for Facebook Ads data:

### Utility Macros
- `calculate_performance_metrics`: Calculate CTR, CPC, CPM, conversion rate, ROAS, cost per conversion
- `classify_performance_tier`: Classify performance into tiers (High/Good/Average/Poor/No Spend)
- `standardize_campaign_objective`: Standardize Facebook campaign objectives to consistent names
- `validate_facebook_ads_data`: Generate data quality validation flags
- `generate_facebook_ads_surrogate_key`: Generate consistent surrogate keys for different grains

### Custom Tests
- `facebook_ads_ctr_range`: Test CTR values are within 0-100% range
- `facebook_ads_metric_consistency`: Test clicks never exceed impressions
- `facebook_ads_spend_consistency`: Test spend consistency with activity
- `facebook_ads_performance_tier_valid`: Test performance tier values are valid

**Usage Example**:
```sql
select 
    date_day,
    campaign_id,
    impressions,
    clicks,
    spend,
    {{ calculate_performance_metrics('impressions', 'clicks', 'spend') }},
    {{ classify_performance_tier('click_through_rate') }} as performance_tier
from my_facebook_ads_data
```

## 📚 Additional Resources

- **Package Capabilities**: Review `analysis/docs/package_capabilities.md` for feature documentation
- **Field Mapping**: Review `analysis/docs/field_mapping.md` for field documentation
- **Macros Documentation**: Review `analysis/docs/macros_documentation.md` for detailed macro usage and examples
- **Data Profiling**: Use `analysis/windsor_data_profiling.sql` to understand your data
- **Source Documentation**: Review `models/staging/facebook_ads/sources.yml` for field definitions
- **Model Documentation**: Check schema.yml files in each layer for model and column documentation
- **Validation Queries**: Use `analysis/validation_*.sql` files for ongoing data consistency monitoring

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📊 Exchange Rate Data

The `data/exchange_rates.csv` file contains sample exchange rate data that has been generated for demonstration and testing purposes. This data should not be used for production financial calculations or real world currency conversions. For production use, please replace with actual exchange rate data from authoritative financial sources.

## 📄 License

This project is licensed under the MIT License.