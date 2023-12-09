{{
    config(materialized = 'table')
}}
select
    id as opportunity_id,
    deal_name as opportunity_name,
    create_date as opportunity_created_on,
    crdt.date_day as opportunity_created_date,
    crdt.week_of_year as opportunity_created_week,
    crdt.month_of_year as opportunity_created_month,
    crdt.quarter_of_year as opportunity_created_quarter,
    close_date as expected_close_ts,
    cldt.date_day as opportunity_expected_close_date,
    cldt.week_of_year as opportunity_expected_close_week,
    cldt.month_of_year as opportunity_expected_close_month,
    cldt.quarter_of_year as opportunity_expected_close_quarter,
    deal_stage_name as opportunity_stage_name,
    pipeline as pipeline_type,
    amount as total_pipeline_value,
    tcv as tcv_pipline_value,
    acv as acv_pipeline_value,
    arr as arr_pipeline_value,
    mrr as mrr_pipeline_value,
    deal_probability
    --observed_date
from
    {{ ref('deals')}} d
    join {{ ref('dim_date')}} crdt
        on cast(d.create_date as date) = crdt.date_day
    join {{ ref('dim_date')}} cldt
        on cast(d.close_date as date) = cldt.date_day
