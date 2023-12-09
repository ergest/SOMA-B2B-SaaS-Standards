{{
    config(materialized = 'view')
}}

with cte_fct_ae_opens_new_opportunity as (
    select
        d.id as opportunity_id,
        d.owner_id,
        cast(d.create_date as date) as activity_date,
        d.create_date as activity_ts,
        1 as fact_value,
        false as is_revenue_impacting
    --    observation_date
    from
        {{ ref('deals') }} d
)
select
    fct.opportunity_id,
    fct.owner_id,
    fct.activity_ts,
    fct.activity_date,
    fct.fact_value,
    fct.is_revenue_impacting,
    mdt.week_of_year as activity_week,
    mdt.month_of_year as activity_month,
    mdt.quarter_of_year as activity_quarter,
    {{ dbt_utils.star(from=ref('dim_opportunity'), except=['opportunity_id'], relation_alias='dopp') }},
    {{ dbt_utils.star(from=ref('dim_owner'), except=['owner_id'], relation_alias='dow') }}
    --observation_date
from
    cte_fct_ae_opens_new_opportunity fct
    join {{ ref('dim_date' )}} mdt
        on fct.activity_date = mdt.date_day
    join {{ ref('dim_opportunity') }} dopp
        on fct.opportunity_id = dopp.opportunity_id
        --and fct.observation_date = dopp.observation_date
    left join {{ ref('dim_owner') }} dow
        on fct.owner_id = dow.owner_id
        --and fct.observation_date = dow.observation_date
