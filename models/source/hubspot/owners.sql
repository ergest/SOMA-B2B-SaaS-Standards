select 
    id,
    email,
    firstName as first_name,
    lastName as last_name,
    userId as user_id,
    cast(createdAt as timestamp) as created_at,
    cast(updatedAt as timestamp) as updated_at,
    archived
from {{ source('raw_hubspot', 'owners') }}