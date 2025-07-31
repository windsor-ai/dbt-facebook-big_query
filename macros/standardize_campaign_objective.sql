{% macro standardize_campaign_objective(objective_col) %}
    {#
        Standardize Facebook campaign objectives to consistent naming
        
        Args:
            objective_col (str): Column name containing campaign objective
            
        Returns:
            SQL case statement with standardized objective names
    #}
    
    case 
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'LEAD_GENERATION' then 'Lead Generation'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'CONVERSIONS' then 'Conversions'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'TRAFFIC' then 'Traffic'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'BRAND_AWARENESS' then 'Brand Awareness'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'REACH' then 'Reach'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'VIDEO_VIEWS' then 'Video Views'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'MESSAGES' then 'Messages'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'APP_INSTALLS' then 'App Installs'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'EVENT_RESPONSES' then 'Event Responses'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'LINK_CLICKS' then 'Link Clicks'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'LOCAL_AWARENESS' then 'Local Awareness'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'OFFER_CLAIMS' then 'Offer Claims'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'OUTCOME_APP_PROMOTION' then 'Outcome App Promotion'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'OUTCOME_AWARENESS' then 'Outcome Awareness'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'OUTCOME_ENGAGEMENT' then 'Outcome Engagement'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'OUTCOME_LEADS' then 'Outcome Leads'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'OUTCOME_SALES' then 'Outcome Sales'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'OUTCOME_TRAFFIC' then 'Outcome Traffic'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'PAGE_LIKES' then 'Page Likes'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'POST_ENGAGEMENT' then 'Post Engagement'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'PRODUCT_CATALOG_SALES' then 'Product Catalog Sales'
        when upper(trim(coalesce({{ objective_col }}, ''))) = 'STORE_VISITS' then 'Store Visits'
        else coalesce({{ objective_col }}, 'Unknown')
    end

{% endmacro %}