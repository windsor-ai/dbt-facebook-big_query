{% macro validate_facebook_ads_data(date_col, account_id_col, campaign_id_col, ad_id_col, impressions_col, clicks_col, spend_col, frequency_col=none) %}
    {#
        Generate data quality validation flags for Facebook Ads data
        
        Args:
            date_col (str): Column name for date
            account_id_col (str): Column name for account ID
            campaign_id_col (str): Column name for campaign ID  
            ad_id_col (str): Column name for ad ID
            impressions_col (str): Column name for impressions
            clicks_col (str): Column name for clicks
            spend_col (str): Column name for spend
            frequency_col (str, optional): Column name for frequency
            
        Returns:
            SQL case statement with data quality flags
    #}
    
    case 
        when {{ date_col }} is null or {{ account_id_col }} is null or {{ campaign_id_col }} is null or {{ ad_id_col }} is null 
            then 'Missing Key Fields'
        when {{ spend_col }} < 0 or {{ clicks_col }} < 0 or {{ impressions_col }} < 0 
            then 'Negative Metrics'
        when {{ clicks_col }} > {{ impressions_col }} and {{ impressions_col }} > 0 
            then 'Invalid CTR'
        {%- if frequency_col %}
        when {{ frequency_col }} < 0 
            then 'Invalid Frequency'
        {%- endif %}
        else 'Valid'
    end

{% endmacro %}