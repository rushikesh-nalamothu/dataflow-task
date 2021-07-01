import re
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)



class CustomParsing(beam.DoFn):
    """ Custom ParallelDo class to apply a custom transformation """

    def to_runner_api_parameter(self, unused_context):
        # Not very relevant, returns a URN (uniform resource name) and the payload
        return "beam:transforms:custom_parsing:custom_v0", None

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
    
        parsed = element.decode('utf-8')
        print('----------------------------------------------------------------------')
        # print(parsed)
        # print(type(parsed))


        values = re.split(":", re.sub('\r\n', '', re.sub('"', '',
                                                         parsed)))
        row = dict(
            zip(('Student_Name', 'RollNo', 'RegNo','Branch','Address1','Address2'),
                values))

        yield row


def run():
   
    # Creating pipeline options
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    # Defining our pipeline and its steps
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription="projects/training-317008/subscriptions/assignment-sub", timestamp_attribute=None
            )
            | "CustomParse" >> beam.ParDo(CustomParsing())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
              table='training-317008:test.test2'
           
            )
        )


if __name__ == "__main__":
    run()