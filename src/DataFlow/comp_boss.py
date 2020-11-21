import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'my-project-1536110405564'
DATASET_ID = 'dotzbigtest'
TABLE = 'comp_boss'
GSURI = 'gs://dotztest/rawzn/comp_boss.csv'
SCHEMA = 'component_id:STRING,component_type_id:STRING,type:STRING,connection_type_id:STRING,outside_shape:STRING,base_type:STRING,height_over_tube:STRING,bolt_pattern_long:STRING,bolt_pattern_wide:STRING,groove:STRING,base_diameter:STRING,shoulder_diameter:STRING,unique_feature:STRING,orientation:STRING,weight:STRING'


def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['component_id']) > 0


def convert_types(data):
    """Converts string values to their appropriate type."""
    data['component_id'] = str(data['component_id']) if 'component_id' in data else None
    data['component_type_id'] = str(data['component_type_id']) if 'component_type_id' in data else None
    data['type'] = str(data['type']) if 'type' in data else None
    data['connection_type_id'] = str(data['connection_type_id']) if 'connection_type_id' in data else None
    data['outside_shape'] = str(data['outside_shape']) if 'outside_shape' in data else None
    data['base_type'] = str(data['base_type']) if 'base_type' in data else None
    data['height_over_tube'] = str(data['height_over_tube']) if 'height_over_tube' in data else None
    data['bolt_pattern_long'] = str(data['bolt_pattern_long']) if 'bolt_pattern_long' in data else None
    data['bolt_pattern_wide'] = str(data['bolt_pattern_wide']) if 'bolt_pattern_wide' in data else None
    data['groove'] = str(data['groove']) if 'groove' in data else None
    data['base_diameter'] = str(data['base_diameter']) if 'base_diameter' in data else None
    data['shoulder_diameter'] = str(data['shoulder_diameter']) if 'shoulder_diameter' in data else None
    data['unique_feature'] = str(data['unique_feature']) if 'unique_feature' in data else None
    data['orientation'] = str(data['orientation']) if 'orientation' in data else None
    data['weight'] = str(data['weight']) if 'weight' in data else None
  
    return data

def del_unwanted_cols(data):
    """Delete the unwanted columns"""
    del data['ibu']
    del data['brewery_id']
    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText(GSURI, skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"component_id": x[0],"component_type_id": x[1],"type": x[2],"connection_type_id": x[3],"outside_shape": x[4],"base_type": x[5],"height_over_tube": x[6],"bolt_pattern_long": x[7],"bolt_pattern_wide": x[8],"groove": x[9],"base_diameter": x[10],"shoulder_diameter": x[11],"unique_feature": x[12],"orientation": x[13],"weight": x[14]}) 
       | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       | 'ChangeDataType' >> beam.Map(convert_types)
       #| 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:{1}.{2}'.format(PROJECT_ID,DATASET_ID,TABLE),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
		   
    result = p.run()