
from pyspark.sql import functions as F


def ingest_bronze(spark, dataset):
    """Incrementally ingest one dataset using Auto Loader."""

    options = {
        **dataset.csv_options,
        **dataset.autoloader_options,
        "cloudFiles.schemaLocation": dataset.schema_location,
        "rescuedDataColumn": "_rescued_data"
    }

    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**options)
        .schema(dataset.bronze_struct_type)
        .load(dataset.config["platform"]["volumes"]["raw_source_root"])
    )

    df = (
        df.withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_source_file_name", F.col("_metadata.file_name"))
    )

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", dataset.checkpoint_location)
        .trigger(availableNow=True)
        .toTable(dataset.full_bronze_table)
    )

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))

    return dataset.full_bronze_table
