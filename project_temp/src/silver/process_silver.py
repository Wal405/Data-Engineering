
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from src.spark_setup import init_spark
BRONZE = ROOT / "data" / "bronze_storage"
SILVER = ROOT / "data" / "silver_storage"


def read(spark, name):
    return spark.read.parquet(str(BRONZE / name))
def save(df, name):
    df.write.mode("overwrite").parquet(str(SILVER / name))
    print(f"{name}: {df.count()} rows")

# --------------------------------------------------
# 1. ROSTER AND ATHLETE HISTORY
# --------------------------------------------------

def build_roster(spark):
    roster = (
        read(spark, "bronze_roster")
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
        .withColumn("max_hr", F.col("max_hr").cast("int"))
        .withColumn("rest_hr", F.col("rest_hr").cast("int"))
    )
    save(roster, "silver_roster_snapshots")

    july = roster.filter(
        F.col("snapshot_date") == "2026-07-01"
    )
    september = roster.filter(
        F.col("snapshot_date") == "2026-09-01"
    )
    
    comparison = (
        september.alias("s")
        .join(july.alias("j"), "athlete_id", "left")
        .select(
            "athlete_id",
            F.col("j.name").alias("old_name"),
            F.col("j.device_id").alias("old_device"),
            F.col("j.squad").alias("old_squad"),
            F.col("s.name").alias("new_name"),
            F.col("s.device_id").alias("new_device"),
            F.col("s.squad").alias("new_squad")
        )
    )

    changed = comparison.filter(
        F.col("old_name").isNull()
        | ~F.col("old_device").eqNullSafe(F.col("new_device"))
        | ~F.col("old_squad").eqNullSafe(F.col("new_squad"))
    )

    # Read Bronze vendor sessions.
    sessions = (
        read(spark, "bronze_vendor_sessions")
        .withColumn("session_date", F.to_date("start_time"))
    )

    # Find first observed use of new devices.
    device_dates = (
        changed
        .filter(
            F.col("old_name").isNotNull()
            & ~F.col("old_device").eqNullSafe(F.col("new_device"))
        ).alias("c")
        .join(
            sessions.alias("v"),
            F.col("c.new_device") == F.col("v.device_id"),
            "inner"
        )
        .groupBy("c.athlete_id")
        .agg(F.min("v.session_date").alias("device_date"))
    )
    # Read Bronze coach labels.
    coach = read(spark, "bronze_coach_labels")
    def normalize(col):
        return F.lower(
            F.regexp_replace(F.trim(col), r"\s+", " ")
        )
    # Match full names and abbreviated names.
    athletes = roster.select("athlete_id", "name").distinct()
    parts = F.split(normalize(F.col("name")), r"\s+")
    full_names = athletes.select(
        "athlete_id",
        normalize(F.col("name")).alias("clean_name")
    )
    short_names = (
        athletes
        .filter(F.size(parts) >= 2)
        .select(
            "athlete_id",
            F.concat(
                parts.getItem(0),
                F.lit(" "),
                F.substring(parts.getItem(1), 1, 1),
                F.lit(".")
            ).alias("clean_name")
        )
    )
    name_lookup = full_names.unionByName(short_names).distinct()
    coach = (
        coach
        .withColumn("session_date", F.to_date("session_date"))
        .withColumn("clean_name", normalize(F.col("athlete_name")))
        .join(name_lookup, "clean_name", "inner")
    )
    # Find first coach label for new squad or new athlete.
    coach_dates = (
        changed.alias("c")
        .join(
            coach.alias("l"),
            (F.col("c.athlete_id") == F.col("l.athlete_id"))
            & (
                (F.col("c.new_squad") == F.col("l.squad"))
                | F.col("c.old_name").isNull()
            ),
            "inner"
        )
        .groupBy("c.athlete_id")
        .agg(F.min("l.session_date").alias("coach_date"))
    )
    changes = (
        changed
        .join(device_dates, "athlete_id", "left")
        .join(coach_dates, "athlete_id", "left")
        .select(
            "athlete_id",
            F.when(
                F.col("old_name").isNull(),
                F.col("coach_date")
            ).when(
                ~F.col("old_device").eqNullSafe(F.col("new_device")),
                F.col("device_date")
            ).otherwise(
                F.col("coach_date")
            ).alias("change_date")
        )
    )
    if changes.filter(F.col("change_date").isNull()).limit(1).count():
        raise ValueError("Could not detect date for a roster change.")
    # Test: Display detected dates.
    print("Detected roster changes:")
    changes.orderBy("athlete_id").show(truncate=False)
    # Fail clearly if the required September records are missing.
    missing = (
        changes.select("athlete_id")
        .join(
            september.select("athlete_id").distinct(),
            "athlete_id",
            "left_anti"
        )
    )
    if missing.limit(1).count():
        raise ValueError(
            "September roster is missing a changed athlete."
        )
    old = (
        july.alias("r")
        .join(changes.alias("c"), "athlete_id", "left")
        .select(
            "athlete_id",
            "r.name",
            "r.device_id",
            "r.squad",
            "r.max_hr",
            "r.rest_hr",
            F.lit("2026-07-01").cast("date").alias("valid_from"),
            F.when(
                F.col("c.change_date").isNotNull(),
                F.date_sub("c.change_date", 1)
            ).otherwise(
                F.lit("2026-12-31").cast("date")
            ).alias("valid_to")
        )
    )
    new = (
        september.alias("r")
        .join(changes.alias("c"), "athlete_id")
        .select(
            "athlete_id",
            "r.name",
            "r.device_id",
            "r.squad",
            "r.max_hr",
            "r.rest_hr",
            F.col("c.change_date").alias("valid_from"),
            F.lit("2026-12-31").cast("date").alias("valid_to")
        )
    )
    history = old.unionByName(new)

    # Check for overlapping ownership of the same device.
    a = history.alias("a")
    b = history.alias("b")
    overlaps = a.join(
        b,
        (F.col("a.device_id") == F.col("b.device_id")) &
        (F.col("a.athlete_id") != F.col("b.athlete_id")) &
        (F.col("a.valid_from") <= F.col("b.valid_to")) &
        (F.col("b.valid_from") <= F.col("a.valid_to")),
        "inner"
    )
    if overlaps.limit(1).count():
        raise ValueError(
            "Overlapping device ownership found in athlete history."
        )
    save(history, "silver_athlete_history")

    snapshot = history.filter(
        (F.col("valid_from") <= F.lit("2026-08-14").cast("date")) &
        (F.col("valid_to") >= F.lit("2026-08-14").cast("date"))
    )
    save(snapshot, "silver_roster_aug14")
    return history


# --------------------------------------------------
# 2. COACH LABELS
# --------------------------------------------------


def build_coach(spark):

    # Read Bronze coach data
    coach = read(spark, "bronze_coach_labels")

    # Read Silver roster data
    roster = read(spark, "silver_roster_snapshots")

    # Normalize athlete names
    def normalize(col):
        return F.lower(
            F.regexp_replace(F.trim(col), r"\s+", " ")
        )

    # Get unique athletes
    athletes = (
        roster
        .select("athlete_id", "name")
        .dropDuplicates()
    )

    # Create full-name lookup
    full_names = (
        athletes
        .withColumn(
            "clean_name",
            normalize(F.col("name"))
        )
        .select("athlete_id", "clean_name")
    )

    # Create abbreviated names
    # Example: Ali Raza -> ali r.
    parts = F.split(normalize(F.col("name")), r"\s+")

    aliases = (
        athletes
        .filter(F.size(parts) >= 2)
        .withColumn(
            "clean_name",
            F.concat(
                parts.getItem(0),
                F.lit(" "),
                F.substring(parts.getItem(1), 1, 1),
                F.lit(".")
            )
        )
        .select("athlete_id", "clean_name")
    )

    # Combine full names and aliases
    athlete_lookup = (
        full_names
        .unionByName(aliases)
        .dropDuplicates()
    )

    # Check for ambiguous names
    ambiguous = (
        athlete_lookup
        .groupBy("clean_name")
        .agg(
            F.countDistinct("athlete_id").alias("id_count")
        )
        .filter(F.col("id_count") > 1)
    )
    if ambiguous.limit(1).count() > 0:
        raise ValueError(
            "Ambiguous athlete names found in roster."
        )
    # Match coach data with roster
    coach = (
        coach
        .withColumn(
            "clean_name",
            normalize(F.col("athlete_name"))
        )
        .join(
            athlete_lookup,
            on="clean_name",
            how="left"
        )
        .drop("clean_name")
        .withColumn(
            "session_date",
            F.to_date("session_date")
        )
        .withColumn(
            "entered_at",
            F.to_timestamp("entered_at")
        )
        .withColumn(
            "rpe",
            F.col("rpe").cast("int")
        )
        .withColumn(
            "workout_type",
            F.col("label")
        )
    )
    # Check unmatched athlete names
    unmatched = coach.filter(
        F.col("athlete_id").isNull()
    )
    if unmatched.limit(1).count() > 0:
        print("Warning: Unmatched coach athlete names")
        unmatched.select(
            "athlete_name"
        ).distinct().show(truncate=False)
    # Rank labels by latest entered_at
    w = Window.partitionBy(
        "athlete_id",
        "session_date"
    ).orderBy(
        F.col("entered_at").desc_nulls_last()
    )
    coach = coach.withColumn(
        "label_rank",
        F.row_number().over(w)
    )
    # Save Silver coach data
    save(coach, "silver_coach_labels")
    return coach
# --------------------------------------------------
# 3. VENDOR SESSIONS AND CONFLICTS
# --------------------------------------------------

def build_vendor(spark):
    vendor = (
        read(spark, "bronze_vendor_sessions")
        .withColumn(
            "duration_s", F.col("raw_duration_s").cast("int")
        )
        .withColumn(
            "distance_m", F.col("raw_distance_m").cast("double")
        )
    )

    counts = vendor.groupBy("session_id").agg(
        F.count("*").alias("record_count"),
        F.countDistinct("duration_s").alias("duration_versions"),
        F.countDistinct("distance_m").alias("distance_versions")
    )

    conflicts = (
        vendor.join(counts, "session_id")
        .filter(F.col("record_count") > 1)
    )

    save(conflicts, "silver_vendor_conflicts")

    # Selection policy: prefer original page over retry.
    # Conflicting versions remain available in the audit.
    w = Window.partitionBy("session_id").orderBy(
        F.col("source_file").contains("retry").asc(),
        F.col("fetched_at").desc_nulls_last(),
        F.col("source_file"),
        F.col("session_index")
    )

    selected = (
        vendor
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    save(selected, "silver_vendor_sessions")
    return selected


# --------------------------------------------------
# 4. BUILD SESSIONS
# --------------------------------------------------

def build_sessions(vendor, history, coach):
    vendor = (
        vendor
        .withColumn("start_ts", F.to_timestamp("start_time"))
        .withColumn("end_ts", F.to_timestamp("end_time"))
        .withColumn(
            "session_date_local", F.to_date("start_ts")
        )
        .withColumn(
            "week_start_monday",
            F.to_date(F.date_trunc("week", "start_ts"))
        )
        .withColumn(
            "week_end_sunday",
            F.date_add("week_start_monday", 6)
        )
    )

    sessions = (
        vendor.alias("v")
        .join(
            history.alias("h"),
            (F.col("v.device_id") == F.col("h.device_id")) &
            (F.col("v.session_date_local") >= F.col("h.valid_from")) &
            (F.col("v.session_date_local") <= F.col("h.valid_to")),
            "left"
        )
        .select(
            "v.*",
            F.col("h.athlete_id").alias("athlete_id"),
            F.col("h.name").alias("athlete_name"),
            F.col("h.squad").alias("squad"),
            F.col("h.max_hr").alias("max_hr"),
            F.col("h.rest_hr").alias("rest_hr")
        )
    )

    w = Window.partitionBy(
        "athlete_id", "session_date_local"
    ).orderBy("start_ts", "session_id")

    sessions = sessions.withColumn(
        "session_rank", F.row_number().over(w)
    )

    sessions = (
        sessions.alias("s")
        .join(
            coach.alias("c"),
            (F.col("s.athlete_id") == F.col("c.athlete_id")) &
            (F.col("s.session_date_local") == F.col("c.session_date")) &
            (F.col("s.session_rank") == F.col("c.label_rank")),
            "left"
        )
        .select(
            "s.*",
            F.col("c.workout_type").alias("workout_type"),
            F.col("c.rpe").alias("rpe")
        )
    )

    return sessions


# --------------------------------------------------
# 5. CLEAN AND LINK DEVICE SAMPLES
# --------------------------------------------------

def build_samples(spark, sessions):
    raw = read(spark, "bronze_samples")

    w = Window.partitionBy(
        "device_id", "sample_time"
    ).orderBy(
        "drop", "uploaded_at", "source_file", "sample_index"
    )

    ranked = raw.withColumn(
        "rn", F.row_number().over(w)
    )

    save(
        ranked.filter(F.col("rn") > 1),
        "silver_sample_duplicates"
    )

    samples = (
        ranked
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn(
            "sample_ts", F.to_timestamp("sample_time")
        )
        .withColumn(
            "invalid_hr",
            F.col("hr").isNull() |
            (F.col("hr") < 30) |
            (F.col("hr") > 240)
        )
        .withColumn(
            "invalid_cadence",
            F.col("cadence").isNull() |
            (F.col("cadence") < 0) |
            (F.col("cadence") > 300)
        )
        .withColumn(
            "invalid_gps",
            (F.col("lat").isNull() != F.col("lon").isNull()) |
            (F.col("lat").isNotNull() & (F.abs("lat") > 90)) |
            (F.col("lon").isNotNull() & (F.abs("lon") > 180))
        )
    )

    joined = (
        samples.alias("p")
        .join(
            sessions.alias("s"),
            (F.col("p.device_id") == F.col("s.device_id")) &
            (F.col("p.sample_ts") >= F.col("s.start_ts")) &
            (F.col("p.sample_ts") < F.col("s.end_ts")),
            "inner"
        )
        .select(
            "p.*",
            F.col("s.session_id").alias("session_id"),
            F.col("s.athlete_id").alias("athlete_id"),
            F.col("s.athlete_name").alias("athlete_name"),
            F.col("s.squad").alias("squad"),
            F.col("s.max_hr").alias("max_hr"),
            F.col("s.end_ts").alias("end_ts")
        )
    )

    w = Window.partitionBy("session_id").orderBy("sample_ts")

    joined = (
        joined
        .withColumn("next_ts", F.lead("sample_ts").over(w))
        .withColumn(
            "nominal_step_s",
            F.when(
                F.col("firmware").startswith("2.4"), 0.5
            ).otherwise(1.0)
        )
        .withColumn(
            "gap_to_next_s",
            F.col("next_ts").cast("double") - F.col("sample_ts").cast("double")
        )
        .withColumn(
            "covered_seconds",
            F.greatest(
                F.lit(0.0),
                F.least(
                    F.col("nominal_step_s"),
                    F.col("end_ts").cast("double") - F.col("sample_ts").cast("double"),
                    F.coalesce(
                        F.col("gap_to_next_s"),
                        F.col("nominal_step_s")
                    )
                )
            )
        )
        .withColumn(
            "internal_gap_seconds",
            F.greatest(
                F.coalesce(
                    F.col("gap_to_next_s"), F.lit(0.0)
                ) - F.col("nominal_step_s"),
                F.lit(0.0)
            )
        )
        .withColumn(
            "invalid_hr",
            F.col("invalid_hr") |
            (F.col("max_hr").isNotNull() &
             (F.col("hr") > F.col("max_hr")))
        )
        .withColumn(
            "hr_zone",
            F.when(
                F.col("invalid_hr") |
                F.col("max_hr").isNull(), None
            )
            .when(F.col("hr") < F.col("max_hr") * 0.60, "Z1")
            .when(F.col("hr") < F.col("max_hr") * 0.70, "Z2")
            .when(F.col("hr") < F.col("max_hr") * 0.80, "Z3")
            .when(F.col("hr") < F.col("max_hr") * 0.90, "Z4")
            .otherwise("Z5")
        )
    )

    save(joined, "silver_session_samples")
    return joined


# --------------------------------------------------
# 6. SESSION QUALITY
# --------------------------------------------------

def build_quality(sessions, samples):
    agg = samples.groupBy("session_id").agg(
        F.sum(
            F.when(
                F.col("hr_zone").isNotNull(),
                F.col("covered_seconds")
            ).otherwise(0.0)
        ).alias("valid_hr_seconds"),
        F.sum("covered_seconds").alias("observed_seconds"),
        F.sum("internal_gap_seconds").alias("internal_gap_seconds")
    )

    quality = (
        sessions
        .join(agg, "session_id", "left")
        .fillna(0.0, [
            "valid_hr_seconds",
            "observed_seconds",
            "internal_gap_seconds"
        ])
        .withColumn(
            "missing_seconds",
            F.greatest(
                F.col("duration_s") - F.col("valid_hr_seconds"),
                F.lit(0.0)
            )
        )
        .withColumn(
            "completeness_pct",
            F.when(
                F.col("duration_s") > 0,
                F.round(
                    F.least(
                        F.col("valid_hr_seconds") /
                        F.col("duration_s"),
                        F.lit(1.0)
                    ) * 100, 1
                )
            )
        )
        .withColumn(
            "quality_flag",
            F.when(
                F.col("completeness_pct") >= 80, "PASS"
            )
            .when(
                F.col("completeness_pct") >= 60, "WARN"
            )
            .otherwise("FAIL")
        )
        .withColumn(
            "quality_notes",
            F.when(
                F.col("duration_s").isNull() |
                (F.col("duration_s") <= 0),
                "Invalid session duration"
            )
            .when(
                F.col("observed_seconds") == 0,
                "No matched sensor samples"
            )
            .when(
                F.col("quality_flag") == "FAIL",
                "Low valid HR coverage"
            )
            .when(
                F.col("quality_flag") == "WARN",
                "Partial valid HR coverage"
            )
            .otherwise("Sufficient valid HR coverage")
        )
        .select(
            "session_id",
            "athlete_id",
            "athlete_name",
            "squad",
            F.col("session_date_local").alias("session_date"),
            "duration_s",
            "valid_hr_seconds",
            "observed_seconds",
            "missing_seconds",
            "internal_gap_seconds",
            "completeness_pct",
            "quality_flag",
            "quality_notes"
        )
    )

    save(quality, "silver_session_quality")
    return quality


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    spark = init_spark("StrideSilver")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Karachi")

    try:
        history = build_roster(spark)
        coach = build_coach(spark)
        vendor = build_vendor(spark)

        sessions = build_sessions(vendor, history, coach)

        # Every session ID must be unique.
        duplicates = (
            sessions.groupBy("session_id")
            .count()
            .filter(F.col("count") > 1)
        )

        if duplicates.limit(1).count():
            raise ValueError("Duplicate session IDs in Silver sessions")

        save(sessions, "silver_sessions")

        samples = build_samples(spark, sessions)
        quality = build_quality(sessions, samples)

        if quality.count() != sessions.count():
            raise ValueError("Session quality row count mismatch")

        print("Silver processing completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
