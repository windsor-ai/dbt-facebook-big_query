{% test facebook_ads_ctr_range(model, column_name, min_value=0, max_value=100) %}
    {#
        Test that click-through rate values are within valid range (0-100%)
        
        Usage:
        - name: facebook_ads_ctr_range
          config:
            where: "impressions > 0"
    #}
    
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and (
          {{ column_name }} < {{ min_value }}
          or {{ column_name }} > {{ max_value }}
      )

{% endtest %}