{{
    config (
        materialized = 'table'
    )
}}
with cte_opportunity_history as (
    {{ get_previous_value_from_entity_history(ref('ent_opportunities_history'),'estimated_pipeline_value') }}
),
cte_latest_employee_history as (
    {{ get_latest_entity_history(ref('ent_employees_history')) }}
)
select
    o.entity_id || o.created_on as primary_key,
    'ae_updates_pipeline_value_for_business_opportunity' as activity_name,
    '{
        "entity":"Account_Executive",
        "entity_id":"'|| ae.entity_id ||'",
        "entity_history_id":"'|| ae.entity_history_id ||'"
     }' as actor,
    '{
        "entity":"New_Business_Opportunity",
        "entity_id":"'|| o.entity_id ||'",
        "entity_history_id":"'|| o.entity_history_id ||'"
     }' as object,
    '{
        "estimated_pipeline_value_current:"'|| o.current_value ||'",
        "estimated_pipeline_value_previous":"'|| o.previous_value ||'"
     }' as json_payload,
    o.created_on as activity_ts,
    o.observed_on,
    o.primary_contact_id,
    o.company_id,
    'estimated_pipeline_value' as activity_attribute,
    o.current_value as activity_value
from
    cte_opportunity_history o
    left join cte_latest_employee_history ae
        on o.owner_id = ae.entity_id
        and ae.observed_on = o.observed_on
