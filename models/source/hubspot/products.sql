select 
    createdate as create_date,
    description,
    hs_cost_of_goods_sold as cost_of_goods_sold,
    hs_lastmodifieddate as last_modified_date,
    hs_recurring_billing_period as recurring_billing_period,
    hs_sku as sku,
    name,
    price,
    hubspot_owner_id as owner_id,
    hs_object_id as id,
    hs_timestamp as time_stamp
from {{ source('raw_hubspot', 'products') }}