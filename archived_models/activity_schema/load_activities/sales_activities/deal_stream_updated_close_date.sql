{{-
    config(materialized = 'view')
-}}

select
    dh.hs_object_id || dh.timestamp as activity_id,
    dh.hs_object_id as entity_id,
    dh.timestamp as activity_ts,
    'updated_close_date' as activity,
    0 as revenue_impact,
    '{ "owner":"' || coalesce(firstName, '') || ' ' || coalesce(lastName, '') || '",' ||
        '"current_close_date":"' || coalesce(closedate, '') || '",' ||
        '"stage":"' || coalesce(dealstage, '') || '"' ||
    '}' as feature_json

from
    {{ source('crm','deals') }} d
    join {{ source('crm','deals_history')}} dh
        on d.hs_object_id = dh.hs_object_id
    left join {{ source('crm','owners') }} o
        on d.hubspot_owner_id = o.id
where
    property_name = 'closedate'
