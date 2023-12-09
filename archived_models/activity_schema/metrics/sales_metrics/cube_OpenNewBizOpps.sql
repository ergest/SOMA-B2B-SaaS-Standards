{{
    config(materialized = 'table')
}}

with cte_data_prep as (
    select
        activity_ts,
        revenue_impact,
        activity,
        feature_json:owner as owner,
        feature_json:stage as stage,
        try_cast(feature_json:current_close_date as timestamp) as current_close_date
    from
        {{ ref('deal_activity_stream') }} m
    where
        activity = 'created_new_opportunity'
),
cte_metrics as (
{{
    generate_metrics_cube (
        source_cte = 'cte_data_prep',
        anchor_date = 'activity_ts',
        metric_calculation = 'count(*)',
        metric_slices = [
                ['owner'],
                ['stage']
        ],
        date_slices = ['week', 'month', 'quarter'],
        include_overall_total = true
    )
}}
)
select *
from cte_metrics
