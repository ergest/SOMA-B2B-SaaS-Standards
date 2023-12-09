{{-
    config(materialized = 'view')
-}}

with cte_deal_history as (
    select
        hs_object_id as deal_id,
        value as current_close_date,
        timestamp,
        lag(value, 1) over(partition by hs_object_id order by `timestamp` asc) as previous_close_date
    from
        {{ source('crm','deals_history') }}
    where
        property_name = 'closedate'
),
cte_pipeline_value_at_that_time as (
    select
        hp.deal_id,
        hp.quote_timestamp,
        hp.pipeline_value,
        row_number() over(partition by hp.deal_id order by timestamp asc) = 1 as is_earliest_pipeline_change
    from
        cte_deal_history dh
        join {{ ref('hs_historical_pipeline_value') }} hp
            on hp.deal_id = dh.deal_id
            and hp.quote_timestamp >= previous_close_date
            and hp.quote_timestamp <= current_close_date 
)
select
    d.hs_object_id || dh.timestamp as activity_id,
    d.hs_object_id as entity_id,
    coalesce(dh.timestamp, d.createdate) as activity_ts,
    'ae_pulled_in_opportunity' as activity,
    0 as revenue_impact,
    '{ "owner":"' || coalesce(firstName, '') || ' ' || coalesce(lastName, '') || '",' ||
        '"current_close_date":"' || coalesce(current_close_date, '') || '",' ||
        '"previous_close_date":"' || coalesce(previous_close_date, '') || '",' ||
        '"stage":"' || coalesce(dealstage, '') || '",' ||
        '"pipeline_value":"' || coalesce(pv.pipeline_value::string, '') || '"' ||
    '}' as feature_json
from
    cte_deal_history dh
    join {{ source('crm','deals') }} d
        on d.hs_object_id = dh.deal_id
    left join {{ source('crm','owners') }} o
        on d.hubspot_owner_id = o.id
    left join cte_pipeline_value_at_that_time pv
        on pv.deal_id = dh.deal_id
        and is_earliest_pipeline_change = true
where
    try_cast(current_close_date as date) < try_cast(previous_close_date as date)
