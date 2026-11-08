
import sys
import argparse
from pathlib import Path

from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.spark_setup import init_spark
from src.silver.process_silver import build_sessions

SILVER = ROOT / "data" / "silver_storage"


def read(spark, name):
    return spark.read.parquet(str(SILVER / name))


def same_rows(left, right):
    """Compare all rows, including duplicate occurrences."""
    return (
        left.exceptAll(right).limit(1).count() == 0
        and right.exceptAll(left).limit(1).count() == 0
    )


def reprocess_day(spark, target_date):
    target = F.to_date(F.lit(target_date))

    # Existing Silver data.
    original = read(spark, "silver_sessions")
    history = read(spark, "silver_athlete_history")
    coach = read(spark, "silver_coach_labels")
    vendor = read(spark, "silver_vendor_sessions")

    # Reuse the same session transformation as Silver.
    rebuilt = build_sessions(vendor, history, coach)

    refreshed_day = rebuilt.filter(
        F.col("session_date_local") == target
    ).select(*original.columns)

    # Preserve all other dates, including rows with null dates.
    other_days = original.filter(
        F.col("session_date_local").eqNullSafe(target) == False
    )

    # Validate rebuilt sessions.
    if refreshed_day.filter(
        F.col("session_date_local") != target
    ).limit(1).count():
        raise ValueError("Rebuild included an incorrect date")

    if (
        refreshed_day.count()
        != refreshed_day.select("session_id").distinct().count()
    ):
        raise ValueError("Duplicate session IDs in rebuilt day")

    # Create complete staged result.
    combined = other_days.unionByName(refreshed_day)

    staging = SILVER / "silver_sessions_reprocessed"

    combined.write.mode("overwrite").parquet(str(staging))

    # Verify the persisted staging output.
    verified = spark.read.parquet(str(staging))

    other_days_after = verified.filter(
        F.col("session_date_local").eqNullSafe(target) == False
    )

    target_after = verified.filter(
        F.col("session_date_local") == target
    )

    if not same_rows(other_days, other_days_after):
        raise ValueError("Other dates changed during reprocessing")

    if not same_rows(refreshed_day, target_after):
        raise ValueError("Refreshed date differs from staged output")

    print("\nSINGLE-DAY REPROCESSING RESULTS")
    print("Target date:", target_date)
    print("Refreshed sessions:", refreshed_day.count())
    print("Other-day sessions:", other_days.count())
    print("Other-day comparison: PASS")
    print("Target-day staging comparison: PASS")
    print("Staging path:", staging)
    print("Original Silver tables: UNCHANGED")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-08-15")
    args = parser.parse_args()

    # Validate the supplied date.
    from datetime import date
    date.fromisoformat(args.date)

    spark = init_spark("StrideReprocessDay")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Karachi")

    try:
        reprocess_day(spark, args.date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
