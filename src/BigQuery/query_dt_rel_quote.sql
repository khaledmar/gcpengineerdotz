SELECT *
FROM
(
SELECT
  tube_assembly_id,
  perioddate,
  bracket_pricing,
  annual_usage,
  quantity,
  SUM(cost) AS cost
  
FROM
  `my-project-1536110405564.dotzbigtestdw.price_quote` q
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.period` p
ON
  p.periodid = q.periodid
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.tube_assembly` t
ON
  t.tubeuid = q. tubeuid
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.supplier` s
ON
  s.supplieruid = q.supplieruid
  
WHERE bracket_pricing = "Yes"
 
 GROUP BY
 tube_assembly_id,
  perioddate,
  annual_usage,
  quantity,
  bracket_pricing

UNION ALL
  
SELECT
  tube_assembly_id,
  bracket_pricing,
  perioddate,
  annual_usage,
  min_order_quantity,
  SUM(cost) AS cost
  
FROM
  `my-project-1536110405564.dotzbigtestdw.price_quote` q
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.period` p
ON
  p.periodid = q.periodid
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.tube_assembly` t
ON
  t.tubeuid = q. tubeuid
INNER JOIN
  `my-project-1536110405564.dotzbigtestdw.supplier` s
ON
  s.supplieruid = q.supplieruid
  
WHERE bracket_pricing = "No"
 
 GROUP BY
 tube_assembly_id,
  perioddate,
  annual_usage,
  min_order_quantity,
  bracket_pricing
 )