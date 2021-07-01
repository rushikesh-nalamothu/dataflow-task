import apache_beam as beam
from apache_beam.io import ReadFromText
import sys
import re


class dataingestion:

    def parse_method(self, string_input):


        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',
                                                         string_input)))
        #print(values,'00000000000000000000000000000')
        row = dict(
            zip(('Name', 'Age', 'Address'),
                values))
        print(row)
        return row

def run():
  argv = [
    #'--project={0}'.format(PROJECT),
    '--staging_location=gs://dataflow-task/staging/',
    '--temp_location=gs://dataflow-task/staging/',
    #'--runner=DataflowRunner'
  ]

  p = beam.Pipeline(argv=argv) 
  data_ingestion = dataingestion()


  (p | 'Read' >> ReadFromText('gs://dataflow-task/data.csv', skip_header_lines=1)
  
  
      |'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))

     

      | "Write to BQ" >> beam.io.WriteToBigQuery(
          table='training-317008:test.one',
         
      )
  )


  p.run()

if __name__ == '__main__':
   run()