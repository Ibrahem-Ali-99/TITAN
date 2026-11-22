{% macro active_publication_id() %}
(
    select warehouse_publication_id
    from {{ source('warehouse', 'wh_active') }}
    limit 1
)
{% endmacro %}

{% macro candidate_filter(column_name) %}
{% if var('warehouse_publication_id', '') == '' %}
where 1 = 0
{% else %}
where {{ column_name }} = '{{ var("warehouse_publication_id") }}'
{% endif %}
{% endmacro %}
