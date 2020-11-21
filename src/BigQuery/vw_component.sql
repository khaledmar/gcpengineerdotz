SELECT
  GENERATE_UUID() AS componentuid,
  component_id,
  component_type_id,
  type,
  connection_type_id,
  outside_shape,
  base_type,
  IFNULL(SAFE_CAST(height_over_tube AS FLOAT64),
    0) AS height_over_tube,
  IFNULL(SAFE_CAST(bolt_pattern_long AS FLOAT64),
    0) AS bolt_pattern_long,
  IFNULL(SAFE_CAST(bolt_pattern_wide AS FLOAT64),
    0) AS bolt_pattern_wide,
  groove,
  IFNULL(SAFE_CAST(base_diameter AS FLOAT64),
    0) AS base_diameter,
  IFNULL(SAFE_CAST(shoulder_diameter AS FLOAT64),
    0) AS shoulder_diameter,
  unique_feature,
  orientation,
  IFNULL(SAFE_CAST(weight AS FLOAT64),
    0) AS weight
FROM
  `my-project-1536110405564.dotzbigtest.comp_boss`
  
  WHERE component_id NOT IN (
  SELECT component_id FROM `my-project-1536110405564.dotzbigtestdw.component`
    )