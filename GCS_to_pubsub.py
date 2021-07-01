
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from google.cloud import pubsub_v1
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
   
    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path('training-317008', 'assignment')

    #for ind in df.index:
    data=element
    #data = "Name : {},Age : {}, Address : {}".format(df['Name'][ind],df['Age'][ind],df['Address'][ind])
    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(future.result())

    print (f"Published messages to {topic_path}.")



def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""

  pipeline_options = PipelineOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  

  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = (p | 'Read' >> ReadFromText('gs://dataflow-task/data.csv', skip_header_lines=1)

        | 'To pubsub' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))

    )



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()