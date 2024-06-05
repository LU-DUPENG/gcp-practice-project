import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions

def word_count(argv=None):
    class WordcountOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                '--input',
                required=True,
                help='Path of the file to read from')
            parser.add_argument(
                '--output',
                required=True,
                help='Output file to write results to.'
        )
        
    class computeLineLengthFn(beam.DoFn):
        def process(self, element):
            return [len(element)]
    
    options=WordcountOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "your project id"
    google_cloud_options.staging_location = "your staging location in cloud storage"
    google_cloud_options.template_location = "/path/to/template folder in the bucket location/myWordCountTemplate"
    google_cloud_options.region = 'your region'
    options.view_as(StandardOptions).runner = 'DataflowRunner' 
    pipeline = beam.Pipeline(options=options)


    _ = (
        pipeline
        | 'read text file' >> beam.io.ReadFromText(options.input)
        | 'compute length' >> beam.ParDo(computeLineLengthFn())
        | 'save the file' >> beam.io.WriteToText(file_path_prefix=options.output, file_name_suffix=".txt")
    )

    result = pipeline.run()
    result.wait_until_finish()
    print(f"Sucessfully create template as {google_cloud_options.template_location}")

if __name__ == '__main__':
  word_count()