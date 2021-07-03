from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
import json
import os

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.pipeline import PipelineOptions
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd


# GCP Service Account Key env 윈도우에서는 환경변수로 설정가능
storage_client = storage.Client()    

# for linux "service account key" 

#GOOGLE_APPLICATION_CREDENTIALS('/home/nasa1515/dataflow/lwskey.json')

# word length code

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

# parser option code
def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',dest='input',required=False,help='default'
        ,default='gs://dataflow_bucket_shivam/input_file_gcs.txt')
    parser.add_argument(
        '--output',dest='output',required=False,help='default'
        ,default='still-chassis-302715:dataflow_practice.table4')
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/still-chassis-302715/subscriptions/students-subscription'
                        )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # pipline option

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'still-chassis-302715'
    google_cloud_options.job_name = 'pubsubgcstobq2'
    google_cloud_options.staging_location = 'gs://temporary_bucket_shivam/staging'
    google_cloud_options.temp_location = 'gs://temporary_bucket_shivam/temporary'
    google_cloud_options.region = 'us-central1'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
        

    # # test1

    p = beam.Pipeline(options = PipelineOptions(pipeline_args))

    with beam.Pipeline(options=pipeline_options) as p:

        table_schema = {
            'fields': [
                {"name": "id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "number", "type": "STRING", "mode": "NULLABLE"},
                {"name": "department", "type": "STRING", "mode": "NULLABLE"},
                {"name": "doj", "type": "STRING", "mode": "NULLABLE"}
            ]
        }
            
        # (p 
        #     | 'Read Data' >> ReadFromText(known_args.input)

        #     | beam.ParDo(WordExtractingDoFn(WordExtractingDoFn))
        #     | 'write to BigQuery' >> beam.io.WriteToBigQuery(
        #         known_args.output,
        #         schema = table_schema,
        #         method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        #         create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #         write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
        #     )
        # )
        p1 = p | "Read from Bucket">>beam.io.ReadFromText(known_args.input)
        p2 = p | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= known_args.inputSubscription) | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
        merged = (p1, p2) | "Merge two" >> beam.Flatten() 
        
        pubsub_data = (
                    merged
                    | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))
                    | 'Split' >> beam.ParDo(Split())
                    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(known_args.output,
                    schema = table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()