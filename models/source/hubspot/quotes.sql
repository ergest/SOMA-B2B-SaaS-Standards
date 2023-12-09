select
    hs_createdate as create_date,
    hs_expiration_date as expiration_date,
    hs_quote_amount as quote_amount,
    hs_quote_number as quote_number,
    hs_status as status,
    hs_terms as terms,
    hs_title as title,
    hubspot_owner_id as owner_id,
    hs_lastmodifieddate as last_modified_date,
    hs_object_id as id
from {{ source('raw_hubspot', 'quotes') }}