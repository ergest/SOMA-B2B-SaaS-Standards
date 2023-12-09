select 
    owner_id,
    team_id,
    team_name,
    team_primary
from {{ source('raw_hubspot', 'owners_teams') }}