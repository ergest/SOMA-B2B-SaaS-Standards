{{-
    config(materialized = 'table')
-}}

with cte_quote_history as  (
    select
        qh.hs_object_id as quote_id,
        d.hs_object_id as deal_id,
        qh.timestamp as quote_timestamp,
        row_number() over(partition by qh.hs_object_id order by qh.timestamp asc)  = 1 as is_initial_value,
        row_number() over(partition by qh.hs_object_id order by qh.timestamp desc) = 1 as is_current_value
    from
         {{ source('crm','quotes_history') }} qh
        join {{ source('crm','associations') }} qtd
            on qtd.from_object_id = qh.hs_object_id 
        join {{ source('crm','deals') }} d 
            on qtd.to_object_id = d.hs_object_id
    where
        qtd.type = 'quote_to_deal'
        and qh.property_name = 'hs_quote_amount'
)
select
    d.hs_object_id as deal_id,
    coalesce(try_cast(li.hs_arr as float), 0.0) as pipeline_value,
    rq.quote_id,
    rq.quote_timestamp
from
    raw_hubspot.deals d
    left join {{ source('crm','associations') }} dtq
        on dtq.from_object_id = d.hs_object_id
    left join cte_quote_history rq
        on dtq.to_object_id = rq.quote_id
    left join {{ source('crm','associations') }} qtli
        on qtli.from_object_id = rq.quote_id
    left join {{ source('crm','line_items') }} li
        on qtli.to_object_id = li.hs_object_id 
where
    dtq.type = 'deal_to_quote'
    and qtli.type = 'quote_to_line_item'
    and lower(li.name) like 'annual%'
