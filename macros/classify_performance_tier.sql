{% macro classify_performance_tier(ctr_col, conversion_rate_col=none, roas_col=none, spend_col='spend') %}
    {#
        Classify performance into tiers based on key metrics
        
        Args:
            ctr_col (str): Column name for click-through rate
            conversion_rate_col (str, optional): Column name for conversion rate
            roas_col (str, optional): Column name for return on ad spend
            spend_col (str): Column name for spend (default: 'spend')
            
        Returns:
            SQL case statement classifying performance tier
    #}
    
    case 
        {%- if conversion_rate_col and roas_col %}
        when {{ ctr_col }} >= 2.0 and {{ conversion_rate_col }} >= 2.0 and {{ roas_col }} >= 3.0 then 'High Performer'
        when {{ ctr_col }} >= 1.0 and {{ conversion_rate_col }} >= 1.0 and {{ roas_col }} >= 2.0 then 'Good Performer'
        when {{ ctr_col }} >= 0.5 and {{ conversion_rate_col }} >= 0.5 and {{ roas_col }} >= 1.0 then 'Average Performer'
        when {{ spend_col }} > 0 then 'Poor Performer'
        {%- else %}
        when {{ ctr_col }} >= 2.0 then 'High Performer'
        when {{ ctr_col }} >= 1.0 then 'Good Performer'
        when {{ ctr_col }} >= 0.5 then 'Average Performer'
        when {{ spend_col }} > 0 then 'Poor Performer'
        {%- endif %}
        else 'No Spend'
    end

{% endmacro %}