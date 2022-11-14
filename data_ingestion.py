import argparse
import time
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

class DataIngestion:
    """Defining a class containing the logic to translate the file into a format BigQuery will accept."""

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.
        Args:
            string_input: A comma separated list of values in the form of
                Sex, Length, Diameter, Height, Whole_weight, Shucked_weight,Viscera_weight,Shell_weight,Rings
                Example string_input: M,0.455,0.365,0.095,0.514,0.2245,0.101,0.15,15
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input. In this example, the data is not transformed, and
            remains in the same format as the CSV.
            example output:
            {
                'Sex': 'M',
                'Length': 0.455,
                'Diameter': 0.365,
                'Height': '0.095,
                'Whole_weight': 0.514,
                'Shucked_weight': 0.2245,
                'Viscera_weight':0.101,
                'Shell_weight':0.15,
                'Rings':15
            }
         """
        # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',
                                                         string_input)))
        row = dict(
            zip(('Sex', 'Length', 'Diameter', 'Height', 'Whole_weight', 'Shucked_weight','Viscera_weight','Shell_weight','Rings'),
                values))
        return row

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from CSV into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    # Static input and output
    input = 'gs://{0}/abalone.csv'.format(opts.project)
    output = '{0}:abalone.abalone_tbl'.format(opts.project)

    # DataIngestion is a class for transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "Sex",
                "type": "STRING"
            },
            {
                "name": "Length",
                "type": "FLOAT"
            },
            {
                "name": "Diameter",
                "type": "FLOAT"
            },
            {
                "name": "Height",
                "type": "FLOAT"
            },
            {
                "name": "Whole_weight",
                "type": "FLOAT"
            },
            {
                "name": "Shucked_weight",
                "type": "FLOAT"
            },
            {
                "name": "Viscera_weight",
                "type": "FLOAT"
            },
            {
                "name": "Shell_weight",
                "type": "FLOAT"
            },
            {
                "name": "Rings",
                "type": "INTEGER"
            }
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)

    '''
    Steps:
    1) Read something
    2) Transform something
    3) Write something
    '''

    (p
        | 'ReadFromGCS' >> beam.io.ReadFromText(input,skip_header_lines=1)
        | 'ParseCsv' >> beam.Map(lambda line: data_ingestion.parse_method(line))
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()