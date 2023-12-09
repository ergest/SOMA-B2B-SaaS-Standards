{{-
    config(materialized = 'table')
-}}

-- Dev's Changes
with prepared_data (
select
        q.hs_object_id as hs_object_id,
        d.hs_object_id as deal_id,
        d.hs_lastmodifieddate as timestamp,
        coalesce(try_cast(d.hs_arr as float), 0) as pipeline_value
    from
         {{ source('crm','quotes') }} q
        join {{ source('crm','associations') }} qtd
            on qtd.to_object_id = q.hs_object_id 
        join {{ source('crm','deals') }} d 
            on qtd.from_object_id = d.hs_object_id
    where
        qtd.type = 'deal_to_quote'
    qualify
        row_number() over(partition by d.hs_object_id order by q.hs_createdate desc) = 1
)
select * 
from prepared_data