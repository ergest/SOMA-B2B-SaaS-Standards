select  
    createdate as create_date,
    hs_lastmodifieddate as last_modified_date,
    hs_product_id as product_id,
    hs_recurring_billing_period as recurring_billing_period,
    name,
    price,
    quantity,
    recurringbillingfrequency as recurring_billing_frequency,
    hs_object_id as id,
    hs_acv as acv,
    hs_arr as arr,
    hs_billing_period_start_date as billing_period_start_date,
    hs_billing_period_end_date as billing_period_end_date,
    hs_cost_of_goods_sold as cost_of_goods_sold,
    hs_created_by_user_id as created_by_user_id
from {{ source('raw_hubspot','line_items') }}