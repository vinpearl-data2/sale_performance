{% macro normalize_string() %}

CREATE
OR REPLACE FUNCTION `{{ target.project ~ "." ~ var("upsell_schema") }}`.normalize_string(word STRING) AS (
  REGEXP_REPLACE(TRIM(UPPER(NORMALIZE(word))), r'\s+', ' ')
);

{% endmacro %}