


import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from google.cloud import pubsub_v1
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
  

    yield element


def run(argv=None, save_main_session=True):

 
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = (p | 'Read' >> ReadFromText('req.txt', skip_header_lines=1)

        | 'Yield' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
      
    | 'Write' >> WriteToText('data',file_name_suffix='.txt',num_shards=1)
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()