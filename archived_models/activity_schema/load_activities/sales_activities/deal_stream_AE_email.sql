{{-
    config(materialized = 'view')
-}}

select
    hs_object_id || hs_createdate as activity_id,
    hs_object_id as entity_id,
    hs_createdate as activity_ts,
    'AE Email' as activity,
    0 as revenue_impact,
    '{"hs_email_direction":"' || coalesce(hs_email_direction, '') || '",' ||
        '"hs_email_status":"' || coalesce(hs_email_status, '') || '"}' as feature_json
from
    {{ source('crm', 'emails') }}

