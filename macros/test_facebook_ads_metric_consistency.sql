{% test facebook_ads_metric_consistency(model, impressions_column='impressions', clicks_column='clicks') %}
    {#
        Test that clicks never exceed impressions in Facebook Ads data
        
        Usage:
        - name: facebook_ads_metric_consistency
          config:
            where: "impressions > 0"
    #}
    
    select *
    from {{ model }}
    where {{ impressions_column }} > 0
      and {{ clicks_column }} > {{ impressions_column }}

{% endtest %}