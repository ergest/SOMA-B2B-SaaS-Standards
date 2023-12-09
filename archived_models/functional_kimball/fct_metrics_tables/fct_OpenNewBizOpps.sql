{{
    config(materialized = 'view')
}}

select
    activity_date as metric_date,
    activity_week as metric_week,
    activity_month as metric_month,
    activity_quarter as metric_quarter,
    opportunity_created_week,
    opportunity_created_month,
    opportunity_created_quarter,
    opportunity_stage_name,
    opportunity_name,
    owner_name,
    'OpenNewBizOpps' as metric_name,
    'sum(fact_value)' as metric_calculation,
    sum(fact_value) as metric_value
    --observation_date
from
    {{ ref('fct_ae_opens_new_opportunity') }}
{{ dbt_utils.group_by(n=11) }}
