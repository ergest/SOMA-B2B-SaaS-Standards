select 
    hs_object_id as id,
    try_cast(coalesce(amount, '0') as double) as amount,
    try_cast(closedate as timestamp) as close_date,
    try_cast(createdate as timestamp) as create_date,
    dealname as deal_name,
    ptos.label as deal_stage_name,
    cast(hs_lastmodifieddate as timestamp) as last_modified_date,
    pipeline,
    hubspot_owner_id as owner_id,
    try_cast(coalesce(hs_tcv, '0') as double) as tcv,
    try_cast(coalesce(hs_closed_amount, '0') as double) as closed_amount,
    try_cast(coalesce(hs_arr, '0') as double) as arr,
    try_cast(coalesce(hs_acv, '0') as double) as acv,
    try_cast(coalesce(hs_mrr, '0') as double) as mrr,
    round(try_cast(hs_deal_stage_probability as double),2) as deal_probability,
    d.archived,
    try_cast(d.archivedAt as timestamp) as archivedAt
from {{ source('raw_hubspot','deals') }} d
left join {{ source('raw_hubspot', 'pipeline_to_stage')}} ptos
on d.dealstage = ptos.id
where d.archived = false