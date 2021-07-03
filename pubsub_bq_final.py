import apache_beam as beam
import argparse
import logging
import csv
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount
from apache_beam.options.pipeline_options import GoogleCloudOptions

# credential_path = "training-gcp.json"

class Split(beam.DoFn):

    def process(self, element):
        from datetime import datetime
        element = element.split(",")
        # d = datetime.strptime(element[1], "%d/%b/%Y:%H:%M:%S")
        # date_string = d.strftime("%Y-%m-%d %H:%M:%S")
        
        return [{ 
            'student_name': element[0],
            'roll_no': element[1],
            'uuid': element[2],
            'branch': element[3],
            'address1': element[4],         
            'address2': element[5],
    
        }]
        
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/still-chassis-302715/subscriptions/students-subscription'
                        )
    parser.add_argument('--output_table',
                        dest='output_table',
                        help='--output_table_name',
                        default='dataflow_practice.table2'
                        )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'still-chassis-302715'
    google_cloud_options.job_name = 'pubsub_to_bq'
    google_cloud_options.staging_location = 'gs://temporary_bucket_shivam/staging'
    google_cloud_options.temp_location = 'gs://temporary_bucket_shivam/temporary'
    google_cloud_options.region = 'us-central1'

    pipeline_options.view_as(StandardOptions).streaming = True
    
    with beam.Pipeline(options=pipeline_options) as p:
        pubsub_data = (
                    p 
                    | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= known_args.inputSubscription)
                    | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))
                    # | 'Split Row' >> beam.Map(lambda row : row.decode('utf-8').split(','))
                    | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
                    | 'ParseCSV' >> beam.ParDo(Split())
                    | 'Write to bq' >> beam.io.WriteToBigQuery(known_args.output_table,
                    schema='student_name:STRING, roll_no:STRING, uuid:STRING, branch:STRING, address1:STRING, address2:STRING', 
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                )
        
        p.run().wait_until_finish()
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()