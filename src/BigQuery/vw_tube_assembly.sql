SELECT
  GENERATE_UUID() AS cotubeuidmponentuid,
  tube_assembly_id
FROM (
  SELECT
    DISTINCT tube_assembly_id
  FROM
    `my-project-1536110405564.dotzbigtest.bill_of_materials`
  WHERE
    tube_assembly_id NOT IN (
    SELECT
      tube_assembly_id
    FROM
      `my-project-1536110405564.dotzbigtestdw.tube_assembly` ) )