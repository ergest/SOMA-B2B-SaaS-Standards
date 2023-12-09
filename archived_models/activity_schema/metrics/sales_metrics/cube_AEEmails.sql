{{
    config(materialized = 'table')
}}

with cte_data_prep as (
    select
        activity_ts,
        revenue_impact,
        activity,
        feature_json:hs_email_direction as email_direction
    from
        {{ ref('deal_stream_AE_email')}}
),
cte_metrics as (
{{
    generate_metrics_cube (
        source_cte = 'cte_data_prep',
        anchor_date = 'activity_ts',
        metric_calculation = 'count(*)',
        metric_slices = [
                ['email_direction']
        ],
        date_slices = ['week','month', 'quarter'],
        include_overall_total = true
    )
}}
)
select *
from cte_metrics 
