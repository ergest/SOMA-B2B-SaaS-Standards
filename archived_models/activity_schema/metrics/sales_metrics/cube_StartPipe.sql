{{
    config(materialized = 'table')
}}

with cte_data_prep as (
    select
        feature_json:current_close_date::timestamp as activity_ts,
        revenue_impact,
        activity,
        feature_json:owner as owner,
        try_cast(feature_json:pipeline_value as float) as pipeline_value,
        feature_json:stage as stage
    from
        {{ ref('deal_activity_stream') }} m
    where
        activity = 'created_new_opportunity'
        and feature_json:stage not in ('closedlost', 'closedwon')
),
cte_metrics as (
{{
    generate_metrics_cube (
        source_cte = 'cte_data_prep',
        anchor_date = 'activity_ts',
        metric_calculation = 'sum(pipeline_value)',
        metric_slices = [
                ['owner'],
                ['stage']
        ],
        date_slices = ['month', 'quarter'],
        include_overall_total = true
    )
}}
)
select *
from cte_metrics
