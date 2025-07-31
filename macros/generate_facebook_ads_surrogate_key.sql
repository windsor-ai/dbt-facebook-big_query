{% macro generate_facebook_ads_surrogate_key(date_col, account_id_col, campaign_id_col, ad_id_col=none, audience_type_col=none, segment_1_col=none, segment_2_col=none) %}
    {#
        Generate surrogate key for Facebook Ads data following consistent patterns
        
        Args:
            date_col (str): Column name for date
            account_id_col (str): Column name for account ID
            campaign_id_col (str): Column name for campaign ID
            ad_id_col (str, optional): Column name for ad ID (for ad-level grain)
            audience_type_col (str, optional): Audience type column (for audience models)
            segment_1_col (str, optional): First segment column (for audience models)
            segment_2_col (str, optional): Second segment column (for audience models)
            
        Returns:
            dbt_utils.generate_surrogate_key macro call with appropriate columns
    #}
    
    {%- set key_columns = [date_col] -%}
    
    {%- if account_id_col -%}
        {%- set _ = key_columns.append(account_id_col) -%}
    {%- endif -%}
    
    {%- if campaign_id_col -%}
        {%- set _ = key_columns.append(campaign_id_col) -%}
    {%- endif -%}
    
    {%- if ad_id_col -%}
        {%- set _ = key_columns.append(ad_id_col) -%}
    {%- endif -%}
    
    {%- if audience_type_col -%}
        {%- set _ = key_columns.append(audience_type_col) -%}
    {%- endif -%}
    
    {%- if segment_1_col -%}
        {%- set _ = key_columns.append(segment_1_col) -%}
    {%- endif -%}
    
    {%- if segment_2_col -%}
        {%- set _ = key_columns.append(segment_2_col) -%}
    {%- endif -%}
    
    {{ dbt_utils.generate_surrogate_key(key_columns) }}

{% endmacro %}