
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from src.spark_setup import init_spark
SILVER = ROOT / "data" / "silver_storage"
BRONZE = ROOT / "data" / "bronze_storage"
GOLD = ROOT / "data" / "gold_marts"
def read(spark, folder, name):
    return spark.read.parquet(str(folder / name))
def save(df, name):
    path = GOLD / name
    df.write.mode("overwrite").parquet(str(path))
    # CSV export for easy inspection.
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(GOLD / f"csv_{name}"))
    )

    print(f"{name}: {df.count()} rows")


# --------------------------------------------------
# MART 1 - WEEKLY TRAINING LOAD
# --------------------------------------------------

def build_weekly(spark):
    sessions = read(spark, SILVER, "silver_sessions")

    weekly = (
        sessions
        .groupBy(
            "week_start_monday",
            "week_end_sunday",
            "athlete_id",
            "athlete_name",
            "squad"
        )
        .agg(
            F.countDistinct("session_id").alias("total_sessions"),
            F.round(
                F.sum("duration_s") / 60.0, 2
            ).alias("logged_duration_minutes"),
            F.round(
                F.sum("distance_m") / 1000.0, 2
            ).alias("total_distance_km")
        )
        .orderBy("week_start_monday", "athlete_name")
    )

    save(weekly, "mart_weekly_training_load")
    return weekly


# --------------------------------------------------
# MART 2 - HR ZONE MINUTES
# --------------------------------------------------

def build_hr_zones(spark):
    sessions = read(spark, SILVER, "silver_sessions")
    samples = read(spark, SILVER, "silver_session_samples")
    quality = read(spark, SILVER, "silver_session_quality")

    zone_columns = [
        F.round(
            F.sum(
                F.when(
                    F.col("hr_zone") == zone,
                    F.col("covered_seconds")
                ).otherwise(0.0)
            ) / 60.0, 2
        ).alias(f"{zone.lower()}_minutes")
        for zone in ["Z1", "Z2", "Z3", "Z4", "Z5"]
    ]

    zones = samples.groupBy("session_id").agg(*zone_columns)

    mart = (
        sessions
        .join(zones, "session_id", "left")
        .join(
            quality.select(
                "session_id",
                "missing_seconds",
                "completeness_pct"
            ),
            "session_id",
            "left"
        )
        .fillna(0.0, subset=[
            "z1_minutes",
            "z2_minutes",
            "z3_minutes",
            "z4_minutes",
            "z5_minutes"
        ])
        .withColumn(
            "missing_hr_minutes",
            F.round(F.col("missing_seconds") / 60.0, 2)
        )
        .withColumn(
            "total_zone_minutes",
            F.round(
                F.col("z1_minutes") +
                F.col("z2_minutes") +
                F.col("z3_minutes") +
                F.col("z4_minutes") +
                F.col("z5_minutes"),
                2
            )
        )
        .select(
            "session_id",
            "athlete_id",
            "athlete_name",
            "squad",
            "device_id",
            F.col("session_date_local").alias("session_date"),
            "workout_type",
            "rpe",
            F.round(
                F.col("duration_s") / 60.0, 2
            ).alias("session_duration_minutes"),
            "z1_minutes",
            "z2_minutes",
            "z3_minutes",
            "z4_minutes",
            "z5_minutes",
            "total_zone_minutes",
            "missing_hr_minutes",
            "completeness_pct"
        )
        .orderBy("session_date", "session_id")
    )

    save(mart, "mart_hr_zone_minutes")
    return mart


# --------------------------------------------------
# MART 3 - SESSION QUALITY
# --------------------------------------------------

def build_quality(spark):
    quality = read(spark, SILVER, "silver_session_quality")

    mart = (
        quality
        .select(
            "session_id",
            "athlete_id",
            "athlete_name",
            "squad",
            "session_date",
            F.round(
                F.col("duration_s") / 60.0, 2
            ).alias("session_duration_minutes"),
            F.round(
                F.col("valid_hr_seconds") / 60.0, 2
            ).alias("valid_hr_minutes"),
            F.round(
                F.col("missing_seconds") / 60.0, 2
            ).alias("missing_hr_minutes"),
            F.round(
                F.col("internal_gap_seconds") / 60.0, 2
            ).alias("internal_gap_minutes"),
            "completeness_pct",
            "quality_flag",
            "quality_notes"
        )
        .orderBy("completeness_pct", "session_date")
    )

    save(mart, "mart_session_quality")
    return mart


# --------------------------------------------------
# MART 4 - AUGUST 14 ROSTER SNAPSHOT
# --------------------------------------------------

def build_roster(spark):
    roster = read(spark, SILVER, "silver_roster_aug14")

    mart = (
        roster
        .select(
            "athlete_id",
            F.col("name").alias("athlete_name"),
            "squad",
            "device_id",
            "max_hr",
            "rest_hr",
            F.lit("2026-08-14").cast("date").alias("snapshot_date")
        )
        .orderBy("squad", "athlete_name")
    )

    save(mart, "mart_roster_snapshot")
    return mart


# --------------------------------------------------
# MART 5 - DATA-DRIVEN DISCREPANCY AUDIT
# --------------------------------------------------

def build_discrepancies(spark):
    batches = read(spark, BRONZE, "bronze_batches")
    duplicates = read(spark, SILVER, "silver_sample_duplicates")
    conflicts = read(spark, SILVER, "silver_vendor_conflicts")
    quality = read(spark, SILVER, "silver_session_quality")
    sessions = read(spark, SILVER, "silver_sessions")
    coach = read(spark, SILVER, "silver_coach_labels")
    baseline = read(spark, GOLD, "mart_athlete_baseline")
    samples = read(spark, SILVER, "silver_session_samples")

    # Declared sample count differs from actual count.
    count_issues = (
        batches
        .filter(F.col("count_mismatch") == True)
        .select(
            F.lit("Device Sync").alias("source"),
            F.lit("Sample count mismatch").alias("category"),
            F.col("batch_id").alias("record_id"),
            F.concat(
                F.lit("Declared: "),
                F.col("declared_samples").cast("string"),
                F.lit("; actual: "),
                F.col("actual_samples").cast("string")
            ).alias("symptom"),
            F.lit("Retain batch and flag mismatch").alias("resolution_action")
        )
    )

    # Duplicate device readings.
    sample_issues = (
        duplicates
        .select(
            F.lit("Device Sync").alias("source"),
            F.lit("Duplicate sample").alias("category"),
            F.concat_ws(
                " | ", "device_id", "sample_time"
            ).alias("record_id"),
            F.lit("Repeated device timestamp").alias("symptom"),
            F.lit(
                "Keep selected sample; preserve duplicate in audit"
            ).alias("resolution_action")
        )
    )

    # Vendor duplicates and conflicting durations.
    vendor_issues = (
        conflicts
        .select(
            F.lit("Vendor API").alias("source"),
            F.when(
                F.col("duration_versions") > 1,
                "Conflicting duration"
            ).otherwise("Duplicate session").alias("category"),
            F.col("session_id").alias("record_id"),
            F.concat(
                F.lit("Duration: "),
                F.coalesce(
                    F.col("duration_s").cast("string"),
                    F.lit("NULL")
                ),
                F.lit(" seconds; file: "),
                F.col("source_file")
            ).alias("symptom"),
            F.lit(
                "Select preferred vendor version; retain alternatives"
            ).alias("resolution_action")
        )
    )

    # Sessions with insufficient valid HR coverage.
    quality_issues = (
        quality
        .filter(F.col("quality_flag") != "PASS")
        .select(
            F.lit("Device Sync").alias("source"),
            F.lit("Incomplete session").alias("category"),
            F.col("session_id").alias("record_id"),
            F.concat(
                F.lit("Completeness: "),
                F.coalesce(
                    F.col("completeness_pct").cast("string"),
                    F.lit("NULL")
                ),
                F.lit("%")
            ).alias("symptom"),
            F.lit(
                "Flag for review; do not fill missing HR"
            ).alias("resolution_action")
        )
    )

    # Sessions without an athlete mapping.
    identity_issues = (
        sessions
        .filter(F.col("athlete_id").isNull())
        .select(
            F.lit("Roster").alias("source"),
            F.lit("Unmapped device").alias("category"),
            F.col("session_id").alias("record_id"),
            F.concat(
                F.lit("Device: "),
                F.col("device_id")
            ).alias("symptom"),
            F.lit(
                "Review effective-dated device ownership"
            ).alias("resolution_action")
        )
    )

    # Coach names that could not be matched.
    coach_issues = (
        coach
        .filter(F.col("athlete_id").isNull())
        .select(
            F.lit("Coach App").alias("source"),
            F.lit("Unmatched athlete name").alias("category"),
            F.col("athlete_name").alias("record_id"),
            F.lit("Name not matched to roster").alias("symptom"),
            F.lit(
                "Review name mapping manually"
            ).alias("resolution_action")
        )
    )

    # Athlete-specific anomaly check using PRIOR days' rolling baselines.
    # This is a review flag, not proof of sensor malfunction.
    # A minimum of 30 historical Z2 running cadence samples is required.
    # The baseline's cadence_count is the number of samples in its 28-day window.
    date_seconds = F.col("observation_date").cast("timestamp").cast("long")
    previous_28_days = (
        Window.partitionBy("athlete_id")
        .orderBy(date_seconds)
        .rangeBetween(-28 * 86400, -86400)
    )

    history = (
        baseline
        .withColumn(
            "prior_typical_cadence",
            F.last("rolling_28d_typical_cadence", ignorenulls=True)
            .over(previous_28_days)
        )
        .withColumn(
            "prior_cadence_count",
            F.last("cadence_count", ignorenulls=True)
            .over(previous_28_days)
        )
        .withColumn(
            "prior_min_observed_hr",
            F.last("rolling_7d_min_observed_hr", ignorenulls=True)
            .over(previous_28_days)
        )
    )

    # Compare the day's own valid Z2 running cadence to the historical norm.
    daily_cadence = (
        samples
        .join(sessions.select("session_id", "sport"), "session_id", "left")
        .filter(
            F.col("athlete_id").isNotNull()
            & F.lower(F.col("sport")).contains("run")
            & (F.col("hr_zone") == "Z2")
            & (F.col("invalid_cadence") == False)
            & (F.col("cadence") > 0)
        )
        .withColumn("observation_date", F.to_date("sample_ts"))
        .groupBy("athlete_id", "observation_date")
        .agg(
              F.median("cadence").alias("daily_median_cadence"),
              F.count("*").alias("daily_cadence_count")
        )
    )

    cadence_issues = (
        history
        .join(daily_cadence, ["athlete_id", "observation_date"], "inner")
        .filter(
            (F.col("prior_cadence_count") >= 30)
            & (F.col("daily_cadence_count") >= 10)
            & (F.col("prior_typical_cadence") > 0)
            & (
                F.abs(F.col("daily_median_cadence") - F.col("prior_typical_cadence"))
                / F.col("prior_typical_cadence") > 0.20
            )
        )
        .select(
            F.lit("Athlete Baseline").alias("source"),
            F.lit("Unusual running cadence").alias("category"),
            F.concat_ws(" | ", "athlete_id", F.col("observation_date").cast("string"))
            .alias("record_id"),
            F.concat(
                F.lit("Daily median cadence: "),
                F.round("daily_median_cadence", 1).cast("string"),
                F.lit("; prior baseline: "),
                F.round("prior_typical_cadence", 1).cast("string")
            ).alias("symptom"),
            F.lit("Investigate cadence deviation; preserve raw readings")
            .alias("resolution_action")
        )
    )

    # Daily minimum observed HR is only a proxy for resting HR.
    # A >25 bpm elevation from a prior 7-day minimum is a review flag.
    hr_issues = (
        history
        .filter(
            F.col("daily_min_hr").isNotNull()
            & F.col("prior_min_observed_hr").isNotNull()
            & ((F.col("daily_min_hr") - F.col("prior_min_observed_hr")) > 25)
        )
        .select(
            F.lit("Athlete Baseline").alias("source"),
            F.lit("Elevated minimum observed HR").alias("category"),
            F.concat_ws(" | ", "athlete_id", F.col("observation_date").cast("string"))
            .alias("record_id"),
            F.concat(
                F.lit("Daily minimum HR: "),
                F.col("daily_min_hr").cast("string"),
                F.lit("; prior 7-day minimum: "),
                F.col("prior_min_observed_hr").cast("string")
            ).alias("symptom"),
            F.lit("Review context and device readings; do not impute HR")
            .alias("resolution_action")
        )
    )

    audit = (
        count_issues
        .unionByName(sample_issues)
        .unionByName(vendor_issues)
        .unionByName(quality_issues)
        .unionByName(identity_issues)
        .unionByName(coach_issues)
        .unionByName(cadence_issues)
        .unionByName(hr_issues)
        .orderBy("source", "category", "record_id")
    )

    save(audit, "mart_discrepancies")
    return audit


# --------------------------------------------------
# README RECONCILIATION
# --------------------------------------------------

def build_reconciliation(spark):
    weekly = read(spark, GOLD, "mart_weekly_training_load")

    target = weekly.filter(
        F.col("week_start_monday") == "2026-08-10"
    )

    expected = spark.createDataFrame([
        ("A01", "Ali Raza", 4),
        ("A02", "Sara Khan", 5),
        ("A03", "Hamza Tariq", 4),
        ("A04", "Zainab Mirza", 4),
        ("A05", "Bilal Ahmed", 4),
        ("A06", "Mehak Noor", 2)
    ], ["athlete_id", "athlete_name", "expected_sessions"])

    actual = target.groupBy("athlete_id").agg(
        F.sum("total_sessions").alias("actual_sessions")
    )

    reconciliation = (
        expected.alias("e")
        .join(actual.alias("a"), "athlete_id", "left")
        .fillna(0, subset=["actual_sessions"])
        .select(
            "athlete_id",
            "e.athlete_name",
            "expected_sessions",
            "actual_sessions",
            (
                F.col("actual_sessions") -
                F.col("expected_sessions")
            ).alias("difference")
        )
        .orderBy("athlete_id")
    )

    save(reconciliation, "reconciliation_sessions")

    endurance = (
        target
        .filter(F.col("squad") == "Endurance")
        .agg(
            F.coalesce(
                F.sum("logged_duration_minutes"),
                F.lit(0.0)
            ).alias("actual_minutes")
        )
        .withColumn("expected_minutes", F.lit(689.0))
        .withColumn(
            "difference",
            F.round(
                F.col("actual_minutes") -
                F.col("expected_minutes"), 2
            )
        )
        .select(
            "expected_minutes",
            "actual_minutes",
            "difference"
        )
    )

    save(endurance, "reconciliation_endurance")

    print("\nSESSION RECONCILIATION")
    reconciliation.show(truncate=False)

    print("\nENDURANCE MINUTES RECONCILIATION")
    endurance.show(truncate=False)


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    spark = init_spark("StrideGold")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Karachi")

    try:
        build_weekly(spark)
        build_hr_zones(spark)
        build_quality(spark)
        build_roster(spark)
        build_discrepancies(spark)
        build_reconciliation(spark)

        print("Gold marts completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
