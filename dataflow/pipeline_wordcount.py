import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    # Configuración del pipeline
    options = PipelineOptions(
        # runner="DataflowRunner",  # Cambiar a DirectRunner para local 
        runner="DirectRunner", 
        # project="gcp-data-engineer-curso-10",
        project="charged-sled-509722-c9",
        region="us-central1",
        temp_location="gs://gcs-bucket-curso-10b/temp"
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Leer archivo" >> beam.io.ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
            | "Separar palabras" >> beam.FlatMap(lambda line: line.split())
            | "Contar palabras" >> beam.combiners.Count.PerElement()
            | "Guardar resultados" >> beam.io.WriteToText("gs://gcs-bucket-curso-10b/output/wordcount")
        )
    print("Pipeline ejecutado exitosamente.")

if __name__ == "__main__":
    run()