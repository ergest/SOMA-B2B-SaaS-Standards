select 
    hs_object_id as id,
    hs_lastmodifieddate as last_modified_date,
    hs_timestamp as time_stamp,
    hubspot_owner_id as owner_id,
    hs_createdate as create_date,
    hs_task_body as task_body,
    hs_task_subject as task_subject,
    hs_task_status as task_status,
    hs_task_priority as task_priority,
    hs_task_type as task_type
from {{ source('raw_hubspot', 'tasks') }}