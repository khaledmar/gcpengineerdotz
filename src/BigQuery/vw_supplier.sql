SELECT
  GENERATE_UUID() AS supplieruid,
  supplier AS supplierdes
FROM (
  SELECT
    DISTINCT supplier
  FROM
    `my-project-1536110405564.dotzbigtest.price_quote`
  WHERE
    supplier NOT IN (
    SELECT
      supplierdes
    FROM
      `my-project-1536110405564.dotzbigtestdw.supplier`))   
    
    
    
  