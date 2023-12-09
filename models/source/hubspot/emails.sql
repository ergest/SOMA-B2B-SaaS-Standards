select 
    hs_object_id as id,
    cast(hs_createdate as timestamp) as create_date,
    cast(hs_lastmodifieddate as timestamp) as last_modified_date,
    cast(hs_timestamp as timestamp) as time_stamp,
    hubspot_owner_id as owner_id,
    hs_email_direction as email_direction,
    hs_email_status as email_status,
    hs_email_from as email_from,
    hs_email_from_firstname as email_from_first_name,
    hs_email_from_lastname as email_from_last_name,
    hs_email_to_email as email_to_email,
    hs_email_to_firstname as email_to_first_name,
    hs_email_to_lastname as email_to_last_name
from {{ source('raw_hubspot','emails') }}