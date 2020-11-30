SELECT
  SAFE_CAST(FORMAT_DATE("%Y%m%d", perioddate) AS INT64) periodid,
  perioddate
FROM (
  SELECT
    DISTINCT SAFE_CAST(quote_date AS DATE) AS perioddate
  FROM
    `my-project-1536110405564.dotzbigtest.price_quote`
    --WHERE SAFE_CAST(quote_date AS DATE) >
    --DATE_SUB(DATE (CURRENT_DATE()), INTERVAL 3 DAY)
    )
WHERE
  perioddate NOT IN (
  SELECT
    perioddate
  FROM
    `my-project-1536110405564.dotzbigtestdw.period`)
    
  