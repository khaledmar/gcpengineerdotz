import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'my-project-1536110405564'
DATASET_ID = 'dotzbigtest'
TABLE = 'bill_of_materials'
GSURI = 'gs://dotztest/rawzn/bill_of_materials.csv'
SCHEMA = 'tube_assembly_id:STRING,component_id_1:STRING,quantity_1:STRING,component_id_2:STRING,quantity_2:STRING,component_id_3:STRING,quantity_3:STRING,component_id_4:STRING,quantity_4:STRING,component_id_5:STRING,quantity_5:STRING,component_id_6:STRING,quantity_6:STRING,component_id_7:STRING,quantity_7:STRING,component_id_8:STRING,quantity_8:STRING'


def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['tube_assembly_id']) > 0


def convert_types(data):
    """Converts string values to their appropriate type."""
    data['tube_assembly_id'] = str(data['tube_assembly_id']) if 'tube_assembly_id' in data else None
    data['component_id_1'] = str(data['component_id_1']) if 'component_id_1' in data else None
    data['quantity_1'] = str(data['quantity_1']) if 'quantity_1' in data else None
    data['component_id_2'] = str(data['component_id_2']) if 'component_id_2' in data else None
    data['quantity_2'] = str(data['quantity_2']) if 'quantity_2' in data else None
    data['component_id_3'] = str(data['component_id_3']) if 'component_id_3' in data else None
    data['quantity_3'] = str(data['quantity_3']) if 'quantity_3' in data else None
    data['component_id_4'] = str(data['component_id_4']) if 'component_id_4' in data else None
    data['quantity_4'] = str(data['quantity_4']) if 'quantity_4' in data else None
    data['component_id_5'] = str(data['component_id_5']) if 'component_id_5' in data else None
    data['quantity_5'] = str(data['quantity_5']) if 'quantity_5' in data else None
    data['component_id_6'] = str(data['component_id_6']) if 'component_id_6' in data else None
    data['quantity_6'] = str(data['quantity_6']) if 'quantity_6' in data else None
    data['component_id_7'] = str(data['component_id_7']) if 'component_id_7' in data else None
    data['quantity_7'] = str(data['quantity_7']) if 'quantity_7' in data else None
    data['component_id_8'] = str(data['component_id_8']) if 'component_id_8' in data else None
    data['quantity_8'] = str(data['quantity_8']) if 'quantity_8' in data else None
  
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
       | 'FormatToDict' >> beam.Map(lambda x: {"tube_assembly_id": x[0],"component_id_1": x[1],"quantity_1": x[2],"component_id_2": x[3],"quantity_2": x[4],"component_id_3": x[5],"quantity_3": x[6],"component_id_4": x[7],"quantity_4": x[8],"component_id_5": x[9],"quantity_5": x[10],"component_id_6": x[11],"quantity_6": x[12],"component_id_7": x[13],"quantity_7": x[14],"component_id_8": x[15],"quantity_8": x[16]}) 
       | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       | 'ChangeDataType' >> beam.Map(convert_types)
       #| 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:{1}.{2}'.format(PROJECT_ID,DATASET_ID,TABLE),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
		   
    result = p.run()