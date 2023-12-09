{{
    config(materialized = 'view')
}}

select
    m.activity_date as metric_date,
    m.activity_week as metric_week,
    m.activity_month as metric_month,
    m.activity_quarter as metric_quarter,
    m.opportunity_created_week,
    m.opportunity_created_month,
    m.opportunity_created_quarter,
    m.opportunity_stage_name,
    m.opportunity_name,
    m.owner_name,
    'OpenSamePdOpps' as metric_name,
    'sum(fact_value)' as metric_calculation,
    sum(fact_value) as metric_value
    --observation_date
from
    {{ ref('fct_ae_opens_new_opportunity') }} m
where
    m.activity_month = m.opportunity_expected_close_month
    or m.activity_quarter = m.opportunity_expected_close_quarter
{{ dbt_utils.group_by(n=11) }}
