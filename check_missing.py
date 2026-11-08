from src.spark_setup import init_spark
from pyspark.sql import functions as F

spark = init_spark("CheckMissingValues")

df = spark.read.parquet(
    "data/silver_storage/silver_session_quality"
)

df.select(
    F.count("*").alias("total_sessions"),
    F.sum("missing_seconds").alias("total_missing_seconds"),
    F.sum("internal_gap_seconds").alias("total_internal_gap_seconds"),
    F.sum(
        F.when(F.col("missing_seconds") > 0, 1).otherwise(0)
    ).alias("sessions_with_missing_data")
).show(truncate=False)

spark.stop()