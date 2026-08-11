{% macro is_weekend(col) -%}
  {%- if target.type == 'athena' -%}
    day_of_week({{ col }}) in (6, 7)
  {%- else -%}
    dayofweek({{ col }}) in (0, 6)
  {%- endif -%}
{%- endmacro %}
