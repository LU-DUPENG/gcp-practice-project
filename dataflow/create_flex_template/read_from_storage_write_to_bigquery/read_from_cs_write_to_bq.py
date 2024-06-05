import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition


def read_from_cs_write_to_bq(argv=None):

    class MyOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument("--input_books_csv", required=True)
            parser.add_argument("--input_libraries_csv", required=True)
            parser.add_argument("--output_bq_table", required=True)
        
    def separate_book(element):
        elements = element.split(",")
        key = elements[2]
        del elements[2]
        return (key, elements)
    
    def separate_library(element):
        elements = element.split(",")
        key = elements[0]
        return (key, [elements[1], ",".join(elements[2:])])
    
    class ExtractBookItem(beam.DoFn):
        def process(self, element):
            value = element[1]
            books = list(value["books"])
            library = list(value["libraries"][0])

            for book in books:
                yield {
                    "book_id": book[0],
                    "title": book[1],
                    "author": book[2],
                    "publish_year": book[3],
                    "library_name": library[0],
                    "library_address": library[1]
                }


    
    options = MyOptions()
    pipeline =beam.Pipeline(options=options)

    books = (
        pipeline
        | "Read books csv" >> beam.io.ReadFromText(options.input_books_csv, skip_header_lines=1)
        | "Deduplicate rows" >> beam.Distinct()
        | "Separate book key-value pair" >> beam.Map(separate_book)
    )

    libraries = (
        pipeline
        | "Read libraries csv" >> beam.io.ReadFromText(options.input_libraries_csv, skip_header_lines=1)
        | "Depulicate rows" >> beam.Distinct()
        | "Separate library key-value pair" >> beam.Map(separate_library)
    ) 

    grouped_book_library = (
        {"books": books, "libraries": libraries}
        | "Join books and libraries" >> beam.CoGroupByKey()
        | "Extract book item" >> beam.ParDo(ExtractBookItem())
    )

    table_schema = {
        'fields': [
            {'name': 'book_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'author', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'publish_year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'library_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'library_address', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }


    grouped_book_library | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        table=options.output_bq_table,
        schema=table_schema,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND
    )

    result = pipeline.run()
    result.wait_until_finish()

if __name__ == '__main__':
  read_from_cs_write_to_bq()

    







