{{-
    config(materialized = 'table')
-}}

with cte_union_streams as (
    -- Returns a list of relations that match schema.prefix%
    {%- set all_tables = dbt_utils.get_relations_by_pattern(target.schema, 'deal_stream%') -%}
    {% for table in all_tables %}
    select
        activity_id,
        entity_id,
        activity,
        activity_ts,
        revenue_impact,
        feature_json
    from
        {{ table }}
    {%- if not loop.last %}
    union all
    {%- endif -%}
    {% endfor %}
)
select
    activity_id,
    entity_id,
    activity,
    activity_ts,
    revenue_impact,
    feature_json,
    row_number() over(partition by entity_id, activity order by activity_ts asc) as activity_occurrence,
    lead(activity_ts, 1) over(partition by entity_id, activity order by activity_ts asc) as activity_repeated_at
from 
    cte_union_streams

--depends on: {{ ref('deal_stream_created_new_opportunity') }}
--depends on: {{ ref('deal_stream_updated_close_date') }}
--depends on: {{ ref('deal_stream_updated_pipeline_value') }}
--depends on: {{ ref('deal_stream_won_deal') }}
--depends on: {{ ref('deal_stream_lost_deal') }}
