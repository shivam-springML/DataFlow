import apache_beam as beam
import argparse
import logging
import csv
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='File to read',
                        default='demo_data.txt')
    parser.add_argument('--output',
                        dest='output',
                        help='Outputfilename',
                        default='gs://dataflow_bucket_shivam/output_demo_data.txt')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        input_rows = p | "Read from TXT">>beam.io.ReadFromText(known_args.input)
        
        # filter_accounts = (
        # input_rows
        # | beam.Map(lambda record: record.split(','))
    
        # )

        p1=(input_rows |"Write to GCS">>beam.io.WriteToText(known_args.output))       
        
        p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()