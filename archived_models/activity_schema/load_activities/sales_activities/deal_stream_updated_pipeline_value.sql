{{-
    config(materialized = 'view')
-}}

select
    dh.hs_object_id || dh.timestamp as activity_id,
    dh.hs_object_id as entity_id,
    dh.timestamp as activity_ts,
    'updated_pipeline_value' as activity,
    0 as revenue_impact,
    '{ "owner":"' || coalesce(firstName, '') || ' ' || coalesce(lastName, '') || 
    '",' || '"pipeline_value":"' || coalesce(dh.value::string, '') || '"' ||
    '}' as feature_json
from
    {{ source('crm', 'deals_history') }} dh
    left join {{ source('crm','deals') }} d
        on dh.hs_object_id = d.hs_object_id
    left join {{ source('crm','owners') }} o
        on d.hubspot_owner_id = o.id
where
    property_name = 'hs_arr'