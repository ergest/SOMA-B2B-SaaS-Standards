{{
    config(materialized = 'table')
}}

with cte_data_prep as (
    select
        activity_id,
        activity_ts,
        revenue_impact,
        activity,
        feature_json:hs_call_direction as call_direction
    from
        {{ ref('deal_stream_AE_call')}}
    where
        feature_json:hs_call_disposition = 'Connected'
),
cte_metrics as (
{{
    generate_metrics_cube (
        source_cte = 'cte_data_prep',
        anchor_date = 'activity_ts',
        metric_calculation = 'count(distinct activity_id)',
        metric_slices = [
                ['call_direction']
        ],
        date_slices = ['week','month', 'quarter'],
        include_overall_total = true
    )
}}
)
select *
from cte_metrics 
