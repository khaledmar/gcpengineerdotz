SELECT
  tubeuid,
  supplieruid,
  periodid,
  SAFE_CAST(annual_usage AS INT64) AS annual_usage,
  SAFE_CAST(min_order_quantity AS INT64) AS min_order_quantity,
  bracket_pricing,
  SAFE_CAST(quantity AS INT64) AS quantity,
  SAFE_CAST(cost AS FLOAT64) AS cost
FROM
  `my-project-1536110405564.dotzbigtest.price_quote` q
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.tube_assembly` t
ON
  t.tube_assembly_id = q.tube_assembly_id
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.supplier` s
ON
  s.supplierdes = q.supplier
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.period` p
ON
  p.periodid = SAFE_CAST(FORMAT_DATE("%Y%m%d", SAFE_CAST(quote_date AS DATE)) AS INT64)