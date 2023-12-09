{{-
    config(materialized = 'view')
-}}

select
    d.hs_object_id || d.closedate as activity_id,
    d.hs_object_id as entity_id,
    d.closedate as activity_ts,
    'lost_deal' as activity,
    0 as revenue_impact,
    '{ "owner":"' || firstName || ' ' || lastName || '",' ||
        '"current_close_date":"' || coalesce(closedate, '') || '",' ||
        '"stage":"' || coalesce(dealstage, '') || '",' ||
        '"pipeline_value":"' || coalesce(d.hs_arr::string, '') || '"' ||
    '}' as feature_json

from
    {{ source('crm','deals') }} d
    left join {{ source('crm','owners') }} o
        on d.hubspot_owner_id = o.id
where 
    dealstage = 'closedlost'
