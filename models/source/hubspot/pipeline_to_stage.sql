select
    pipeline_id,
    label as stage_name,
    id as stage_id,
    display_order,
    createdAt as create_date,
    updatedAt as update_date,
    archived 
from {{ source('raw_hubspot', 'pipeline_to_stage') }}