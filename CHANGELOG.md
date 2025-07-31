# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-07-31

### 🚀 Initial Release

This is the initial release of the **dbt Facebook Ads Windsor Package**  a production ready dbt package that transforms raw Facebook Ads data from Windsor.ai into clean, analytics ready tables in BigQuery.

### ✨ Features

#### **Data Models**
- **4 Mart Models**: 
  - `facebook_ads__base_spend` - Essential spend tracking with core performance metrics
  - `facebook_ads__ad_performance_daily` - Daily ad-level performance metrics
  - `facebook_ads__campaign_summary` - Campaign level aggregated performance with ad distribution insights
  - `facebook_ads__audience_metrics` - Audience performance analytics combining demographics and location data
  
- **2 Intermediate Models**:
  - `int_facebook_ads__currency_normalized` - Multi currency normalization layer
  - `int_facebook_ads__daily_metrics` - Enhanced metrics with performance tiers and quality flags

- **3 Staging Models**:
  - `stg_facebook_ads__campaigns` - Campaign entities with hierarchy
  - `stg_facebook_ads__ads` - Ad creative entities
  - `stg_facebook_ads__insights` - Performance insights with deduplication

#### **Data Sources Supported**
- **Core Facebook Ads Data**: Campaigns, ads, and performance insights from Windsor.ai
- **Audience Data**: Demographics (age, gender) and location (country, region) segmentation
- **Exchange Rates**: Multi currency support with normalization

#### **Macros & Testing**
- **5 Utility Macros**:
  - `calculate_performance_metrics` - Calculate CTR, CPC, CPM, conversion metrics
  - `classify_performance_tier` - Performance tier classification
  - `standardize_campaign_objective` - Campaign objective standardization
  - `validate_facebook_ads_data` - Data quality validation flags
  - `generate_facebook_ads_surrogate_key` - Consistent surrogate key generation

- **6 Custom Tests**:
  - `facebook_ads_ctr_range` - CTR validation (0-100%)
  - `facebook_ads_metric_consistency` - Clicks vs impressions validation
  - `facebook_ads_spend_consistency` - Spend consistency validation
  - `facebook_ads_performance_tier_valid` - Performance tier validation
  - `assert_facebook_ads_data_quality` - Comprehensive data quality test
  - `assert_campaign_summary_aggregation` - Campaign aggregation validation

#### **BigQuery Optimizations**
- **Partitioning**: Date based partitioning on all mart models
- **Clustering**: Strategic clustering by account_id and campaign_id
- **Data Types**: BigQuery native data types (INT64, FLOAT64, STRING)
- **Materialization**: Views for staging/intermediate, tables for marts

#### **Data Quality & Validation**
- **Automated Deduplication**: Intelligent deduplication based on spend and impressions
- **Data Quality Checks**: Missing fields, negative values, logical error validation
- **Performance Classification**: Automated tier assignment (High/Good/Average/Poor/No Spend)
- **Alert System**: Performance alert flags for metrics requiring attention
- **Validation Queries**: 6 SQL validation files for ongoing monitoring

#### **Configuration & Flexibility**
- **Configurable Variables**:
  - `facebook_ads_start_date` - Data processing start date
  - `exclude_test_campaigns` - Filter test campaigns
  - `min_spend_threshold` - Minimum spend filter
  - `min_impressions_threshold` - Minimum impressions filter

#### **Documentation**
- **Comprehensive README** - Installation, configuration, and usage guide
- **Package Capabilities** - Detailed feature documentation
- **Field Mapping** - Complete field documentation and transformations
- **Macro Documentation** - Detailed macro usage with examples
- **Model Documentation** - Every model and column documented with tests

### ⚙️ Technical Implementation

#### **Dependencies**
- `dbt-labs/dbt_utils@1.3.0` - Utility functions
- `metaplane/dbt_expectations@0.10.4` - Advanced testing
- `dbt-labs/audit_helper@0.12.1` - Data comparison utilities

#### **Architecture**
- **3-Layer Design**: Staging → Intermediate → Marts
- **Modular Structure**: Reusable intermediate models
- **Consistent Naming**: Clear prefixes and conventions
- **Performance Optimized**: BigQuery specific optimizations

#### **Data Processing**
- **Incremental Models**: Efficient processing of large datasets
- **Currency Normalization**: Multi currency support with exchange rates
- **Safe Type Casting**: Proper handling of string to numeric conversions
- **Error Handling**: Graceful handling of data quality issues

### 📊 Business Intelligence Features

#### **Performance Analytics**
- **Efficiency Metrics**: CTR, CPC, CPM, conversion rates, ROAS
- **Composite Scoring**: Weighted efficiency scores for performance ranking
- **Trend Analysis**: Time series performance tracking
- **Benchmarking**: Cross campaign and cross ad performance comparison

#### **Audience Insights**
- **Demographics Analysis**: Age and gender performance breakdowns
- **Geographic Analysis**: Country and regional performance insights
- **Segmentation**: Cross dimensional audience analysis
- **Targeting Optimization**: Data driven audience recommendations

#### **Campaign Intelligence**
- **Performance Tiers**: Automated classification system
- **Alert Systems**: Performance anomaly detection
- **Portfolio Analysis**: Cross campaign performance comparison
- **Budget Optimization**: Campaign level efficiency analysis

### 📚 Documentation & Resources

#### **Included Documentation**
- `README.md` - Package documentation
- `analysis/docs/package_capabilities.md` - Feature capabilities guide
- `analysis/docs/field_mapping.md` - Field mapping and transformations
- `analysis/docs/macros_documentation.md` - Macro usage and examples

#### **Analysis Files**
- `analysis/validation_*.sql` - 6 validation queries for data monitoring
- `analysis/windsor_data_profiling.sql` - Data profiling queries
- `data/exchange_rates.csv` - Sample exchange rate data

### 🏗️ Production Readiness

This package is production ready with:
- ✅ **Testing** - 344+ data tests across all models
- ✅ **Performance Optimization** - BigQuery partitioning and clustering
- ✅ **Data Quality Assurance** - Multi layer validation and monitoring
- ✅ **Professional Documentation** - Complete usage and API documentation
- ✅ **Best Practices** - Follows all dbt community best practices
- ✅ **Extensibility** - Modular design for easy customization

### 🛠️ Usage

```yaml
# dbt_project.yml
vars:
  facebook_ads_start_date: '2024-01-01'
  exclude_test_campaigns: true
  min_spend_threshold: 0
  min_impressions_threshold: 1
```

```bash
# Installation
dbt deps
dbt run
dbt test
```

### 🎯 Supported Use Cases

- **Marketing Operations**: Campaign optimization, budget allocation, ROI analysis
- **Business Intelligence**: Executive reporting, KPI tracking, trend analysis
- **Data Analytics**: Customer acquisition analysis, market expansion, competitive benchmarking
- **Performance Marketing**: A/B testing, creative optimization, audience targeting

---

## Development

This package was developed following dbt best practices and is ready for community adoption. For issues, feature requests, or contributions, please visit the GitHub repository.

**Package Statistics**:
- **9 Models** (3 staging, 2 intermediate, 4 marts)
- **5 Sources** (campaigns, ads, insights, audience location, audience demographics)
- **5 Utility Macros** + **6 Custom Tests**
- **344+ Data Tests** across all models
- **Documentation** with usage examples