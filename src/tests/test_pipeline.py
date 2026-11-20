
"""
STRIDE PERFORMANCE LAB - PIPELINE TESTS

Run:
    python src/tests/test_pipeline.py

Idempotency proof:
    python src/tests/test_pipeline.py --snapshot
    # Rerun Bronze, Silver, Gold and baseline with unchanged inputs.
    python src/tests/test_pipeline.py --verify
"""

import sys
import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.spark_setup import init_spark

BRONZE = ROOT / "data" / "bronze_storage"
SILVER = ROOT / "data" / "silver_storage"
GOLD = ROOT / "data" / "gold_marts"
SNAPSHOT = ROOT / "data" / "test_snapshots"

MARTS = [
    "mart_weekly_training_load",
    "mart_hr_zone_minutes",
    "mart_session_quality",
    "mart_roster_snapshot",
    "mart_discrepancies",
    "mart_athlete_baseline",
    "reconciliation_sessions",
    "reconciliation_endurance",
]


def read(spark, folder, name):
    return spark.read.parquet(str(folder / name))


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def same_rows(a, b):
    # Compare all rows, including duplicate occurrences.
    check(a.columns == b.columns, "Column mismatch")
    return (
        a.exceptAll(b).limit(1).count() == 0
        and b.exceptAll(a).limit(1).count() == 0
    )


# TEST 1 - SESSION UNIQUENESS

def test_uniqueness(spark):
    sessions = read(spark, SILVER, "silver_sessions")
    zones = read(spark, GOLD, "mart_hr_zone_minutes")

    for name, df in [
        ("Silver sessions", sessions),
        ("Gold HR zones", zones),
    ]:
        duplicates = (
            df.groupBy("session_id")
            .count()
            .filter(F.col("count") > 1)
        )
        check(
            not duplicates.limit(1).count(),
            f"{name} contains duplicate session IDs"
        )

    print("PASS 1: Session IDs are unique")


# TEST 2 - ONE QUALITY ROW PER SESSION

def test_quality(spark):
    sessions = read(spark, SILVER, "silver_sessions")
    quality = read(spark, GOLD, "mart_session_quality")

    session_ids = sessions.select("session_id")
    quality_ids = quality.select("session_id")

    check(
        same_rows(session_ids, quality_ids),
        "Quality records do not match source sessions 1-to-1"
    )

    print("PASS 2: One quality row per session")


# TEST 3 - DETERMINISTIC VENDOR SELECTION

def test_vendor_selection(spark):
    raw = read(spark, BRONZE, "bronze_vendor_sessions")
    selected = read(spark, SILVER, "silver_vendor_sessions")

    w = Window.partitionBy("session_id").orderBy(
        F.col("source_file").contains("retry").asc(),
        F.col("fetched_at").desc_nulls_last(),
        F.col("source_file"),
        F.col("session_index")
    )

    expected = (
        raw.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            "session_id",
            F.col("raw_duration_s").cast("int").alias("duration_s"),
            "source_file",
            "session_index"
        )
    )

    actual = selected.select(
        "session_id",
        "duration_s",
        "source_file",
        "session_index"
    )

    check(
        same_rows(expected, actual),
        "Vendor selection differs from the documented rule"
    )

    target = selected.filter(
        F.col("session_id") == "Se06bc0e7"
    ).select("duration_s").collect()

    if target:
        print(
            "Conflict Se06bc0e7 selected duration:",
            target[0]["duration_s"],
            "seconds"
        )
    else:
        print("Conflict Se06bc0e7 not present in current input")

    print("PASS 3: Vendor selection follows deterministic rule")


# TEST 4 - HR ZONE CONSERVATION

def test_zone_conservation(spark):
    zones = read(spark, GOLD, "mart_hr_zone_minutes")

    violations = zones.filter(
        (F.col("total_zone_minutes") < -0.01) |
        (
            F.col("total_zone_minutes") >
            F.col("session_duration_minutes") + 0.06
        ) |
        (
            F.col("total_zone_minutes") +
            F.col("missing_hr_minutes") >
            F.col("session_duration_minutes") + 0.12
        )
    )

    check(
        not violations.limit(1).count(),
        "HR-zone minutes exceed session duration"
    )

    print("PASS 4: HR-zone minutes respect session duration")


# TEST 5 - README RECONCILIATION

def test_reconciliation(spark):
    reconciliation = read(
        spark, GOLD, "reconciliation_sessions"
    )

    endurance = read(
        spark, GOLD, "reconciliation_endurance"
    )

    print("\nREADME session reconciliation:")
    reconciliation.show(truncate=False)

    print("README Endurance reconciliation:")
    endurance.show(truncate=False)

    session_mismatches = reconciliation.filter(
        F.col("difference") != 0
    ).count()

    endurance_mismatches = endurance.filter(
        F.abs("difference") > 0.01
    ).count()

    if session_mismatches or endurance_mismatches:
        print(
            "RECONCILIATION WARNING:",
            session_mismatches,
            "athlete count differences;",
            endurance_mismatches,
            "Endurance duration differences."
        )
        print(
            "Investigate source records, device ownership, "
            "and vendor conflicts. Do not force a match."
        )
    else:
        print("README control figures match")

    print("PASS 5: Reconciliation results inspected")


# TEST 6 - IDEMPOTENCY PROOF

def test_idempotency(spark, mode):
    if mode == "snapshot":
        for name in MARTS:
            df = read(spark, GOLD, name)
            df.write.mode("overwrite").parquet(
                str(SNAPSHOT / name)
            )
        print("SNAPSHOT SAVED: Rerun the pipeline with unchanged inputs")
        return

    if mode == "verify":
        for name in MARTS:
            before = read(spark, SNAPSHOT, name)
            after = read(spark, GOLD, name)

            check(
                same_rows(before, after),
                f"Idempotency failed: {name} changed"
            )

        print("PASS 6: All marts identical after pipeline rerun")
        return

    print(
        "NOT RUN 6: Use --snapshot, rerun pipeline, "
        "then --verify for idempotency proof"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot", action="store_true"
    )
    parser.add_argument(
        "--verify", action="store_true"
    )
    args = parser.parse_args()

    if args.snapshot and args.verify:
        parser.error("Choose either --snapshot or --verify")

    mode = (
        "snapshot" if args.snapshot
        else "verify" if args.verify
        else "normal"
    )

    spark = init_spark("StridePipelineTests")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Karachi")

    try:
        test_uniqueness(spark)
        test_quality(spark)
        test_vendor_selection(spark)
        test_zone_conservation(spark)
        test_reconciliation(spark)
        test_idempotency(spark, mode)

        print("\nPipeline checks completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
