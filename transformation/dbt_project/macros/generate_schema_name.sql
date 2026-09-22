{#
  dbt's built-in default appends a model's custom +schema to the target
  schema (e.g. target schema "gates" + custom schema "bronze" -> "gates_bronze"),
  not "bronze" alone. That default is meant for splitting one logical
  warehouse across many physical schemas by environment/team; it actively
  fights the medallion layout here, where "bronze"/"silver"/"gold" must be
  the literal Trino schema names. Override it to use the custom schema
  as-is, falling back to the target schema only when a model sets none.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
