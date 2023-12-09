{{
    config(materialized = 'table')
}}
select 
    id as owner_id,
    email,
    first_name,
    last_name,
    first_name || ', ' || last_name as owner_name,
    created_at
    --owner_team,
    --owner_org,
    --observation_date
from
    {{ ref('owners') }}
