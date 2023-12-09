{{
    config(materialized = 'table')
}}

with cte_data_prep_month as (
    select
        activity_ts,
        revenue_impact,
        activity,
        feature_json:owner as owner,
        feature_json:stage as stage,
        try_cast(feature_json:current_close_date as timestamp) as current_close_date,
        try_cast(feature_json:previous_close_date as timestamp) as previous_close_date
    from
        {{ ref('deal_activity_stream') }}
    where
        activity = 'pulled_in_opportunity'
        and date_trunc('month', try_cast(feature_json:current_close_date as timestamp)) <
            date_trunc('month', try_cast(feature_json:previous_close_date as timestamp))
),
cte_data_prep_quarter as (
    select
        activity_ts,
        revenue_impact,
        activity,
        feature_json:owner as owner,
        feature_json:stage as stage,
        try_cast(feature_json:current_close_date as timestamp) as current_close_date,
        try_cast(feature_json:previous_close_date as timestamp) as previous_close_date
    from
        {{ ref('deal_activity_stream') }}
    where
        activity = 'pulled_in_opportunity'
        and date_trunc('quarter', try_cast(feature_json:current_close_date as timestamp)) <
            date_trunc('quarter', try_cast(feature_json:previous_close_date as timestamp))
),
cte_metrics_month as (
{{
    generate_metrics_cube (
        source_cte = 'cte_data_prep_month',
        anchor_date = 'activity_ts',
        metric_calculation = 'count(*)',
        metric_slices = [
                ['owner'],
                ['stage']
        ],
        date_slices = ['month'],
        include_overall_total = true
    )
}}
),
cte_metrics_quarter as (
{{
    generate_metrics_cube (
        source_cte = 'cte_data_prep_quarter',
        anchor_date = 'activity_ts',
        metric_calculation = 'count(*)',
        metric_slices = [
                ['owner'],
                ['stage']
        ],
        date_slices = ['quarter'],
        include_overall_total = true
    )
}}
)
select *
from cte_metrics_month
union all
select *
from cte_metrics_quarter
