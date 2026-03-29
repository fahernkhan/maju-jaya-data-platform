-- macros/generate_schema_name.sql
-- Override default dbt behavior:
-- Tanpa ini: schema='mart_maju' + profile schema 'stg_maju' = 'stg_maju_mart_maju'
-- Dengan ini: schema='mart_maju' langsung jadi 'mart_maju'

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}