{% macro calculate_performance_metrics(impressions_col, clicks_col, spend_col, conversions_col=none, conversion_value_col=none) %}
    {#
        Calculate standard Facebook Ads performance metrics
        
        Args:
            impressions_col (str): Column name for impressions
            clicks_col (str): Column name for clicks  
            spend_col (str): Column name for spend
            conversions_col (str, optional): Column name for conversions
            conversion_value_col (str, optional): Column name for conversion value
            
        Returns:
            SQL expressions for CTR, CPC, CPM, and optionally conversion metrics
    #}
    
    -- Click-through rate (CTR)
    case 
        when {{ impressions_col }} > 0 then ({{ clicks_col }} / cast({{ impressions_col }} as float64)) * 100.0
        else 0.0
    end as click_through_rate,
    
    -- Cost per click (CPC)
    case 
        when {{ clicks_col }} > 0 then {{ spend_col }} / cast({{ clicks_col }} as float64)
        else null
    end as cost_per_click,
    
    -- Cost per mille (CPM)
    case 
        when {{ impressions_col }} > 0 then ({{ spend_col }} / cast({{ impressions_col }} as float64)) * 1000.0
        else null
    end as cost_per_mille
    
    {%- if conversions_col and conversion_value_col -%}
    ,
    -- Conversion rate
    case 
        when {{ clicks_col }} > 0 then ({{ conversions_col }} / cast({{ clicks_col }} as float64)) * 100.0
        else 0.0
    end as conversion_rate,
    
    -- Cost per conversion
    case 
        when {{ conversions_col }} > 0 then {{ spend_col }} / cast({{ conversions_col }} as float64)
        else null
    end as cost_per_conversion,
    
    -- Return on ad spend (ROAS)
    case 
        when {{ spend_col }} > 0 then {{ conversion_value_col }} / {{ spend_col }}
        else null
    end as return_on_ad_spend
    {%- endif -%}

{% endmacro %}