WITH
  cte_tube_component AS (
  SELECT
    tube_assembly_id,
    component_id,
    SAFE_CAST(quantity AS INT64) AS quantity
  FROM (
    SELECT
      tube_assembly_id,
      component.key AS ckey, 
      quantity.key AS qkey,
      component.value AS component_id,
      quantity.value AS quantity
    FROM
      `my-project-1536110405564.dotzbigtest.bill_of_materials` a,
      UNNEST(dotzbigtest.unpivot(a,
          "component_id_")) component,
      UNNEST(dotzbigtest.unpivot(a,
          "quantity_")) quantity )
  WHERE
    component_id != "NA"
    AND 
    quantity != "NA"
    AND ckey = qkey )
    
    
  SELECT
    a.tubeuid,
    a.tube_assembly_id,
    ARRAY_AGG(STRUCT(b.component_id,
      b.quantity)) AS component
  FROM
    `my-project-1536110405564.dotzbigtest.vw_tube_assembly` a
     LEFT OUTER JOIN cte_tube_component b
     ON  b.tube_assembly_id = a.tube_assembly_id
  #WHERE
   
    #AND a.tube_assembly_id = "TA-10964" 
    GROUP BY a.tubeuid,a.tube_assembly_id