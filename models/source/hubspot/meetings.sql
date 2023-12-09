select 
    hs_object_id as id,
    hs_lastmodifieddate as last_modified_date,
    cast(hs_timestamp as timestamp) as time_stamp,
    hubspot_owner_id as owner_id,
    hs_meeting_title as meeting_title,
    hs_meeting_body as meeting_body,
    hs_internal_meeting_notes as internal_meeting_notes,
    hs_meeting_external_URL as meeting_external_url,
    hs_meeting_location as meeting_location,
    hs_meeting_start_time as meeting_start_time,
    hs_meeting_end_time as meeting_end_time,
    hs_meeting_outcome as meeting_outcome,
    hs_activity_type as activity_type,
    hs_attachment_ids as attachment_ids,
    cast(hs_createdate as timestamp) as create_date
from {{ source('raw_hubspot', 'meetings') }}