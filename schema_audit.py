import sys
from pathlib import Path
from pyspark.sql import functions as F

# Project root
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.spark_setup import init_spark

BRONZE = ROOT / "data" / "bronze_storage"
SILVER = ROOT / "data" / "silver_storage"


def check_a04_samples(spark):

    # Read Silver tables
    samples = spark.read.parquet(
        str(SILVER / "silver_session_samples")
    )

    sessions = spark.read.parquet(
        str(SILVER / "silver_sessions")
    )

    # 1. Check A04 sessions
    print("\n========== A04 SESSIONS ==========\n")

    a04_sessions = sessions.filter(
        F.col("athlete_id") == "A04"
    )

    a04_sessions.select(
        "session_id",
        "athlete_id",
        "device_id",
        "start_ts",
        "end_ts"
    ).show(50, truncate=False)

    print("Total A04 sessions:", a04_sessions.count())

    # 2. Check matched Silver samples
    print("\n========== A04 SENSOR SAMPLES ==========\n")

    a04_samples = samples.filter(
        F.col("athlete_id") == "A04"
    )

    sample_summary = (
        a04_samples
        .groupBy("session_id")
        .agg(
            F.count("*").alias("sample_count"),
            F.sum(
                F.when(
                    F.col("invalid_hr") == False, 1
                ).otherwise(0)
            ).alias("valid_hr_samples"),
            F.sum(
                F.when(
                    F.col("hr_zone").isNotNull(), 1
                ).otherwise(0)
            ).alias("zoned_samples")
        )
    )

    sample_summary.show(50, truncate=False)

    print("Total A04 samples:", a04_samples.count())

    # 3. Find sessions without samples
    print("\n========== A04 SESSIONS WITHOUT SAMPLES ==========\n")

    missing = (
        a04_sessions.select("session_id")
        .join(
            a04_samples.select("session_id").distinct(),
            "session_id",
            "left_anti"
        )
    )

    missing.show(50, truncate=False)

    print("Sessions without samples:", missing.count())

    # 4. Check HR validity
    print("\n========== A04 HR VALIDITY ==========\n")

    a04_samples.groupBy(
        "invalid_hr",
        "hr_zone"
    ).count().orderBy(
        "invalid_hr",
        "hr_zone"
    ).show(50, truncate=False)

    # 5. Read Bronze samples
    print("\n========== D014 BRONZE SAMPLES ==========\n")

    bronze = spark.read.parquet(
        str(BRONZE / "bronze_samples")
    )

    d014 = bronze.filter(
        F.col("device_id") == "D014"
    )

    d014.agg(
        F.count("*").alias("total_samples"),
        F.min("sample_time").alias("first_sample"),
        F.max("sample_time").alias("last_sample")
    ).show(truncate=False)

    # 6. Display D014 sample examples
    print("\n========== D014 SAMPLE EXAMPLES ==========\n")

    d014.select(
        "device_id",
        "sample_time",
        "hr",
        "firmware"
    ).show(10, truncate=False)

    # 7. Test timestamp offsets
    print("\n========== D014 TIMESTAMP OFFSET TEST ==========\n")

    d014_samples = d014.withColumn(
        "sample_ts",
        F.to_timestamp("sample_time")
    )

    for hours in [0, 1, 2, 3, 5, -2]:

        shifted = d014_samples.withColumn(
            "test_ts",
            F.expr(f"sample_ts + INTERVAL {hours} HOURS")
        )

        matched = shifted.alias("p").join(
            a04_sessions.alias("s"),
            (
                (F.col("p.device_id") == F.col("s.device_id"))
                & (F.col("p.test_ts") >= F.col("s.start_ts"))
                & (F.col("p.test_ts") < F.col("s.end_ts"))
            ),
            "inner"
        )

        print(
            f"Offset {hours:+} hours:",
            matched.count(),
            "matched samples"
        )

    print("\nA04 investigation completed.")


def main():

    spark = init_spark("CheckA04Samples")

    spark.conf.set(
        "spark.sql.session.timeZone",
        "Asia/Karachi"
    )

    try:
        check_a04_samples(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()