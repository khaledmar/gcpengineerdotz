import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'my-project-1536110405564'
DATASET_ID = 'dotzbigtest'
TABLE = 'price_quote'
GSURI = 'gs://dotztest/rawzn/price_quote.csv'
SCHEMA = 'tube_assembly_id:STRING,supplier:STRING,quote_date:STRING,annual_usage:STRING,min_order_quantity:STRING,bracket_pricing:STRING,quantity:STRING,cost:STRING'


def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['tube_assembly_id']) > 0


def convert_types(data):
    """Converts string values to their appropriate type."""
    data['tube_assembly_id'] = str(data['tube_assembly_id']) if 'tube_assembly_id' in data else None
    data['supplier'] = str(data['supplier']) if 'supplier' in data else None
    data['quote_date'] = str(data['quote_date']) if 'quote_date' in data else None
    data['annual_usage'] = str(data['annual_usage']) if 'annual_usage' in data else None
    data['min_order_quantity'] = str(data['min_order_quantity']) if 'min_order_quantity' in data else None
    data['bracket_pricing'] = str(data['bracket_pricing']) if 'bracket_pricing' in data else None
    data['quantity'] = str(data['quantity']) if 'quantity' in data else None
    data['cost'] = str(data['cost']) if 'cost' in data else None
  
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
       | 'FormatToDict' >> beam.Map(lambda x: {"tube_assembly_id": x[0],"supplier": x[1],"quote_date": x[2],"annual_usage": x[3],"min_order_quantity": x[4],"bracket_pricing": x[5],"quantity": x[6],"cost": x[7]}) 
       | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       | 'ChangeDataType' >> beam.Map(convert_types)
       #| 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:{1}.{2}'.format(PROJECT_ID,DATASET_ID,TABLE),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
		   
    result = p.run()