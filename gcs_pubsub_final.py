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
from google.cloud import pubsub_v1


# GCP Service Account Key env 윈도우에서는 환경변수로 설정가능
storage_client = storage.Client()    

class SendAsBatch(beam.DoFn):
  
  def process(self, element):
    publisher = pubsub_v1.PublisherClient()
    topic_id = "projects/still-chassis-302715/topics/student"
    data=element
    data = data.encode("utf-8")
    future = publisher.publish(topic_id, data)

    print(future.result())

# parser option code
def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',dest='input',required=False,help='default'
        ,default='gs://dataflow_bucket_shivam/input_file.txt')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # pipline option

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'still-chassis-302715'
    google_cloud_options.job_name = 'gcstopubsub'
    google_cloud_options.staging_location = 'gs://temporary_bucket_shivam/staging'
    google_cloud_options.temp_location = 'gs://temporary_bucket_shivam/temporary'
    google_cloud_options.region = 'asia-northeast3'
    pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
        

    # # test1

    p = beam.Pipeline(options = PipelineOptions(pipeline_args))

    with beam.Pipeline(options=pipeline_options) as p:

        
            
        (p 
            | 'Read Data' >> ReadFromText(known_args.input)

            | 'To pubsub' >> (beam.ParDo(SendAsBatch()).with_output_types(str))
            )
        

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()