{% test facebook_ads_spend_consistency(model, spend_column='spend', clicks_column='clicks', impressions_column='impressions') %}
    {#
        Test that spend data is consistent with activity (no spend without impressions/clicks)
        
        Usage:
        - name: facebook_ads_spend_consistency
    #}
    
    select *
    from {{ model }}
    where {{ spend_column }} > 0
      and {{ impressions_column }} = 0
      and {{ clicks_column }} = 0

{% endtest %}