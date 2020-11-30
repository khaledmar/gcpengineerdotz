CREATE OR REPLACE FUNCTION `my-project-1536110405564.dotzbigtest.unpivot`(x ANY TYPE, col_regex STRING) AS (
(
      # https://medium.com/@hoffa/how-to-unpivot-multiple-columns-into-tidy-pairs-with-sql-and-bigquery-d9d0e74ce675
    SELECT
      ARRAY_AGG(STRUCT( REGEXP_EXTRACT(y, '\\d+') AS key,
          REGEXP_EXTRACT(REPLACE(y, '"', ''), r':([^"]*)') AS value
          #,REGEXP_EXTRACT(y, '[^"]*') AS keyon
          #,REPLACE(y, '"', '') AS nu
          #, REGEXP_EXTRACT(REPLACE(y, '"', ''), r':([^"]*)') AS value
          #, REGEXP_EXTRACT(y, r':([^"]*)\"?[,}\]]') AS value
          ))
    FROM
      UNNEST((
        SELECT
          REGEXP_EXTRACT_ALL(json,col_regex||r'[^:]+:\"?[^"]+\"?') arr
        FROM (
          SELECT
            TO_JSON_STRING(x) json))) y )
);