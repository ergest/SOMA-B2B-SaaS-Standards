select
    hs_object_id as id,
    hs_lastmodifieddate as last_modified_date,
    hs_timestamp as time_stamp,
    hubspot_owner_id as owner_id,
    hs_note_body as note_body,
    hs_attachment_ids as attachment_ids,
    hs_createdate as create_date
from {{ source('raw_hubspot', 'notes') }}