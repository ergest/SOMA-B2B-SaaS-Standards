{{-
    config(materialized = 'view')
-}}


select
    d.hs_object_id || d.closedate as activity_id,
    d.hs_object_id as entity_id,
    case when d.closedate = '' then d.hs_lastmodifieddate else d.closedate end as activity_ts,
    'won_deal' as activity,
    0 as revenue_impact,
    '{ "owner":"' || firstName || ' ' || lastName || '",' ||
        '"current_close_date":"' || case when d.closedate = '' then d.hs_lastmodifieddate else d.closedate end || '",' ||
        '"stage":"' || coalesce(dealstage, '') || '",' ||
        '"pipeline_value":"' || coalesce(d.hs_arr::string, '') || '"' ||
    '}' as feature_json

from
    {{ source('crm','deals') }} d
    left join {{ source('crm','owners') }} o
        on d.hubspot_owner_id = o.id
where 
    dealstage = 'closedwon'
    --and closedate = '' 

-- deal_id : 15125028120, has no close date. For that reason we need to use CASE statement and populate hs_lastmodification 