{{-
    config(materialized = 'view')
-}}

select  
    c.hs_object_id || c.hs_createdate as activity_id,
    c.hs_object_id as entity_id,
    c.hs_createdate as activity_ts,
    'AE Calls' as activity,
    0 as revenue_impact,
    '{' '"hs_call_direction":"' || coalesce(c.hs_call_direction,'') || '",' ||
        '"hs_call_disposition":"' || 
            case when coalesce(c.hs_call_disposition, '') = "f240bbac-87c9-4f6e-bf70-924b57d47db7" then "Connected"  
            when coalesce(c.hs_call_disposition, '') = "9d9162e7-6cf3-4944-bf63-4dff82258764" then "Busy" 
            when coalesce(c.hs_call_disposition, '') = "a4c4c377-d246-4b32-a13b-75a56a4cd0ff" then "Left live message"
            when coalesce(c.hs_call_disposition, '') = "b2cf5968-551e-4856-9783-52b3da59a7d0" then "Left voicemail"
            when coalesce(c.hs_call_disposition, '') = "73a0d17f-1163-4015-bdd5-ec830791da20" then "No answer"
            when coalesce(c.hs_call_disposition, '') = "17b47fee-58de-441e-a44c-c6300d46f273" then "Wrong number"
            else coalesce(c.hs_call_disposition, '') end || '",' ||
        '"hs_call_duration":' || coalesce(c.hs_call_duration, '') || '",' ||
        '"deal_id":' || coalesce(a.from_object_id, '') ||
    '}' as feature_json

from {{ source('crm', 'calls')}} c
left join {{ source('crm', 'associations')}} a
    on c.hs_object_id = a.to_object_id and a.type = 'deal_to_call'


-- Findings:
-- Only 52 calls has deal associated.

-- Call_disposition feild (call outcome):
-- Busy: 9d9162e7-6cf3-4944-bf63-4dff82258764
-- Connected: f240bbac-87c9-4f6e-bf70-924b57d47db7
-- Left live message: a4c4c377-d246-4b32-a13b-75a56a4cd0ff
-- Left voicemail: b2cf5968-551e-4856-9783-52b3da59a7d0
-- No answer: 73a0d17f-1163-4015-bdd5-ec830791da20
-- Wrong number: 17b47fee-58de-441e-a44c-c6300d46f273