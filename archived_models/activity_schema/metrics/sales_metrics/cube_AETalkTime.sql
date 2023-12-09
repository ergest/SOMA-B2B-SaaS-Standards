{{
    config(materialized = 'table')
}}

with cte_data_prep as (
    select
        activity_ts,
        revenue_impact,
        activity,
        feature_json:hs_call_direction as call_direction,
        round(try_cast(feature_json:hs_call_duration as bigint) / 60000, 2) as call_duration_in_minutes
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
        metric_calculation = 'sum(call_duration_in_minutes)',
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
