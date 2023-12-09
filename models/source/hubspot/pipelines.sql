select 
    label as pipeline_name,
    display_order,
    pipeline_id,
    createdAt as create_date,
    updatedAt as update_date,
    archived
from {{ source('raw_hubspot', 'pipeline') }}