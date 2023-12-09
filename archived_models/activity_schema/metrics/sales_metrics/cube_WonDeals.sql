{{
    config(materialized = 'table')
}}

with cte_data_prep as (
    select
        activity_ts,
        revenue_impact,
        activity,
        feature_json:owner as owner,
        feature_json:stage as stage
    from
        {{ ref('deal_activity_stream') }} m
    where
        activity = 'won_deal'
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
        date_slices = ['day','month', 'quarter'],
        include_overall_total = true
    )
}}
)
select *
from cte_metrics 
