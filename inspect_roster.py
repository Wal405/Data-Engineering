"""Standalone integration check for Stride Silver athlete roster history.

Run from the project root:
    python tests/test_roster_changes.py

Requires Bronze Parquet inputs and your existing src.pipelines.silver module.
This test calls build_roster(), which overwrites its three Silver roster outputs.
"""

from datetime import date
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F
from src.spark_setup import init_spark
from src.silver.process_silver import build_roster

EXPECTED = {
    "A03": date(2026, 8, 12),
    "A05": date(2026, 8, 12),
    "A06": date(2026, 8, 14),
}


def main():
    spark = init_spark("RosterChangeVerification")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Karachi")
    try:
        history = build_roster(spark)

        print("\n===== DETECTED NEW HISTORY ROWS =====")
        new_rows = history.filter(F.col("valid_from") > F.lit("2026-07-01").cast("date"))
        new_rows.select("athlete_id", "device_id", "squad", "valid_from", "valid_to") \
            .orderBy("athlete_id").show(truncate=False)

        actual = {
            row["athlete_id"]: row["valid_from"]
            for row in new_rows.select("athlete_id", "valid_from").collect()
        }
        assert actual == EXPECTED, f"Wrong change dates. Expected {EXPECTED}, got {actual}"
        print("PASS: Changed athlete IDs and dates match expected results")

        assert history.filter(F.col("valid_from").isNull() | F.col("valid_to").isNull()).count() == 0, \
            "History contains missing validity dates"
        assert history.filter(F.col("valid_from") > F.col("valid_to")).count() == 0, \
            "History contains invalid date ranges"
        print("PASS: History validity dates are complete and ordered")

        # A new athlete must not have a fabricated July history row.
        a06_old = history.filter(
            (F.col("athlete_id") == "A06")
            & (F.col("valid_from") < F.lit("2026-08-14").cast("date"))
        ).count()
        assert a06_old == 0, "A06 is a new athlete but has an incorrect earlier history row"
        print("PASS: New athlete A06 has no pre-join history")

        aug14 = history.filter(
            (F.col("valid_from") <= F.lit("2026-08-14").cast("date"))
            & (F.col("valid_to") >= F.lit("2026-08-14").cast("date"))
        )
        print("\n===== AUGUST 14 ROSTER =====")
        aug14.select("athlete_id", "device_id", "squad").orderBy("athlete_id").show(truncate=False)
        assert aug14.select("athlete_id").distinct().count() == 6, \
            "August 14 snapshot should contain six athletes"
        print("PASS: August 14 snapshot contains six athletes")
        print("\nALL ROSTER TESTS PASSED")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
