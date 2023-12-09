select 
    hash_id,
    type,
    from_object_name,
    from_object_id,
    to_object_name,
    to_object_id
from {{ source('raw_hubspot','associations') }}