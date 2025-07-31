{% test facebook_ads_performance_tier_valid(model, column_name) %}
    {#
        Test that performance tier values are from the expected set
        
        Usage:
        - name: facebook_ads_performance_tier_valid
    #}
    
    select *
    from {{ model }}
    where {{ column_name }} not in (
        'High Performer',
        'Good Performer', 
        'Average Performer',
        'Poor Performer',
        'No Spend'
    )

{% endtest %}