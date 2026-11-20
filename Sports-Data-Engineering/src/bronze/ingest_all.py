
import sys
import argparse
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    ArrayType, IntegerType
)
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from src.spark_setup import init_spark
DATA = ROOT / "data"
BRONZE = DATA / "bronze_storage"

def save(df, name):
    df.write.mode("overwrite").parquet(str(BRONZE / name))
    print(name, df.count())


# Explicit schema handles firmware field variations.
sample_schema = StructType([
    StructField("t", StringType()),
    StructField("hr", StringType()),
    StructField("heart_rate", StringType()),
    StructField("cadence", StringType()),
    StructField("lat", StringType()),
    StructField("lon", StringType()),
    StructField("gps", StructType([
        StructField("lat", StringType()),
        StructField("lon", StringType()),
        StructField("hdop", StringType())
    ]))
])

device_schema = StructType([
    StructField("batch_id", StringType()),
    StructField("device_id", StringType()),
    StructField("firmware", StringType()),
    StructField("app_version", StringType()),
    StructField("uploaded_at", StringType()),
    StructField("sample_count", IntegerType()),
    StructField("samples", ArrayType(sample_schema))
])
def ingest_devices(spark, last_drop):

    paths = [
        str(DATA / "device_sync" / f"drop_{i}" / "*.json")
        for i in range(1, last_drop + 1)
    ]

    raw = (
        spark.read
        .schema(device_schema)
        .option("multiLine", True)
        .json(paths)
        .withColumn("source_file", F.input_file_name())
        .withColumn(
            "drop",
            F.regexp_extract("source_file", r"drop_[123]", 0)
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    batches = (
        raw
        .select(
            "batch_id", "device_id", "firmware",
            "app_version", "uploaded_at", "drop",
            "source_file", "ingested_at",
            F.col("sample_count").alias("declared_samples"),
            F.size("samples").alias("actual_samples")
        )
        .withColumn(
            "count_mismatch",
            F.col("declared_samples") != F.col("actual_samples")
        )
    )

    samples = (
        raw
        .select(
            "batch_id", "device_id", "firmware",
            "app_version", "uploaded_at", "drop",
            "source_file", "ingested_at",
            F.posexplode("samples").alias(
                "sample_index", "sample"
            )
        )
        .select(
            "batch_id", "device_id", "firmware",
            "app_version", "uploaded_at", "drop",
            "source_file", "ingested_at", "sample_index",
            F.col("sample.t").alias("sample_time"),
            F.col("sample.hr").alias("raw_hr"),
            F.col("sample.heart_rate").alias("raw_heart_rate"),
            F.col("sample.cadence").alias("raw_cadence"),
            F.col("sample.lat").alias("raw_lat"),
            F.col("sample.lon").alias("raw_lon"),
            F.to_json("sample.gps").alias("raw_gps"),
            F.coalesce(
                F.col("sample.heart_rate"),
                F.col("sample.hr")
            ).cast("double").alias("hr"),
            F.col("sample.cadence").cast("double").alias("cadence"),
            F.coalesce(
                F.col("sample.gps.lat"),
                F.col("sample.lat")
            ).cast("double").alias("lat"),
            F.coalesce(
                F.col("sample.gps.lon"),
                F.col("sample.lon")
            ).cast("double").alias("lon"),
            F.col("sample.gps.hdop").cast("double").alias("hdop")
        )
    )

    save(batches, "bronze_batches")
    save(samples, "bronze_samples")


def ingest_vendor(spark):

    raw = (
        spark.read
        .option("multiLine", True)
        .json(str(DATA / "vendor_api" / "*.json"))
        .withColumn("source_file", F.input_file_name())
    )

    sessions = (
        raw
        .select(
            "fetched_at", "page", "source_file",
            F.posexplode("sessions").alias(
                "session_index", "session"
            )
        )
        .select(
            "fetched_at", "page", "source_file",
            "session_index",
            F.col("session.session_id").alias("session_id"),
            F.col("session.device_id").alias("device_id"),
            F.col("session.sport").alias("sport"),
            F.col("session.start_time").alias("start_time"),
            F.col("session.end_time").alias("end_time"),
            F.col("session.duration_s").alias("raw_duration_s"),
            F.col("session.distance_m").alias("raw_distance_m"),
            F.col("session.sync_state").alias("sync_state")
        )
    )
    save(sessions, "bronze_vendor_sessions")


def ingest_roster(spark):

    paths = [
        str(DATA / "roster" / "roster_2026-07-01.csv"),
        str(DATA / "roster" / "roster_2026-09-01.csv")
    ]

    roster = (
        spark.read
        .option("header", True)
        .csv(paths)
        .withColumn("source_file", F.input_file_name())
        .withColumn(
            "snapshot_date",
            F.regexp_extract(
                "source_file",
                r"roster_(\d{4}-\d{2}-\d{2})",
                1
            )
        )
    )

    save(roster, "bronze_roster")


def ingest_coach(spark):

    coach = (
        spark.read
        .option("header", True)
        .csv(str(DATA / "coach_app" / "session_labels.csv"))
        .withColumn("source_file", F.input_file_name())
    )

    save(coach, "bronze_coach_labels")


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--drop",
        type=int,
        choices=[1, 2, 3],
        default=3
    )
    args = parser.parse_args()

    spark = init_spark("StrideBronze")

    try:
        ingest_devices(spark, args.drop)
        ingest_vendor(spark)
        ingest_roster(spark)
        ingest_coach(spark)

        print("Bronze ingestion completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
