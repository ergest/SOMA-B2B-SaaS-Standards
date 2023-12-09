select 
    hs_object_id as id,
    hubspot_owner_id as owner_id,
    industry,
    name,
    phone,
    state,
    try_cast(annualrevenue as double) as annual_revenue,
    city,
    country,
    try_cast(createdate as timestamp) as create_date,
    description,
    domain,
    hs_additional_domains as additional_domains,
    try_cast(hs_lastmodifieddate as timestamp) as last_modified_date,
    hs_pipeline as pipeline    
from {{ source('raw_hubspot','companies') }}