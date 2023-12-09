select 
    hs_activity_type as activity_type,
    hs_attachment_ids as attachment_ids,
    hs_call_body as call_body,
    hs_call_callee_object_id as call_callee_object_id,
    hs_call_callee_object_type as call_callee_object_type,
    hs_call_direction as call_direction,
    cast(hs_lastmodifieddate as timestamp) as last_modified_date,
    hs_object_id as id,
    case coalesce(hs_call_disposition, '')
        when "f240bbac-87c9-4f6e-bf70-924b57d47db7" then "Connected"  
        when "9d9162e7-6cf3-4944-bf63-4dff82258764" then "Busy" 
        when "a4c4c377-d246-4b32-a13b-75a56a4cd0ff" then "Left live message"
        when "b2cf5968-551e-4856-9783-52b3da59a7d0" then "Left voicemail"
        when "73a0d17f-1163-4015-bdd5-ec830791da20" then "No answer"
        when "17b47fee-58de-441e-a44c-c6300d46f273" then "Wrong number"
        else coalesce(hs_call_disposition, 'No value')
    end as call_disposition,
    cast(hs_call_duration as int) as call_duration,
    hs_call_from_number as call_from_number,
    hs_call_recording_url as call_recording_url,
    hs_call_status as call_status,
    hs_call_title as call_title,
    hs_call_to_number as call_to_number,
    cast(hs_createdate as timestamp) as create_date,
    cast(hs_timestamp as timestamp) as time_stamp,
    hubspot_owner_id as owner_id,
    hs_call_deal_stage_during_call as call_deal_stage_during_call ,
    hs_created_by as created_by
from
    {{ source('raw_hubspot','calls') }}