select 
    hs_object_id as id,
    property_name,
    value,
    timestamp as time_stamp,
    sourceType as source_type,
    sourceId as source_id,
    updatedByUserId as updated_by_user_id
from {{ source('raw_hubspot','line_items_history') }}