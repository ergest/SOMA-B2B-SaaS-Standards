select 
    hs_object_id as id,
    property_name as property_name,
    value as value,
    timestamp as time_stamp,
    sourceType as source_type,
    sourceId as source_id,
    updatedByUserId as updated_by_user_id
from {{ source('raw_hubspot','deals_history') }}