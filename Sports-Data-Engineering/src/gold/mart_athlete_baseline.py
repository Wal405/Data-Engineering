
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from src.spark_setup import init_spark
SILVER = ROOT / "data" / "silver_storage"
GOLD = ROOT / "data" / "gold_marts"
def build_athlete_baseline(spark):
    samples = spark.read.parquet(
        str(SILVER / "silver_session_samples")
    )

    sessions = spark.read.parquet(
        str(SILVER / "silver_sessions")
    )
    # Get sport from the session table.
    samples = samples.join(
        sessions.select("session_id", "sport"),
        "session_id",
        "left"
    )
    daily = (
        samples
        .filter(F.col("athlete_id").isNotNull())
        .withColumn(
            "observation_date",
            F.to_date("sample_ts")
        )
        .groupBy(
            "athlete_id",
            "observation_date"
        )
        .agg(
            F.first("athlete_name", ignorenulls=True)
             .alias("athlete_name"),

            F.first("squad", ignorenulls=True)
             .alias("squad"),

            F.min(
                F.when(
                    ~F.col("invalid_hr"),
                    F.col("hr")
                )
            ).alias("daily_min_hr"),

            F.collect_list(
                F.when(
                    F.lower(F.col("sport")).contains("run") &
                    (F.col("hr_zone") == "Z2") &
                    (~F.col("invalid_cadence")) &
                    (F.col("cadence") > 0),
                    F.col("cadence")
                )
            ).alias("daily_z2_cadences")
        )
        .withColumn(
            "date_epoch_s",
            F.unix_timestamp("observation_date")
        )
    )

    # Calendar windows include the current day.
    w7 = (
        Window.partitionBy("athlete_id")
        .orderBy("date_epoch_s")
        .rangeBetween(-6 * 86400, 0)
    )

    w28 = (
        Window.partitionBy("athlete_id")
        .orderBy("date_epoch_s")
        .rangeBetween(-27 * 86400, 0)
    )

    baseline = (
        daily
        .withColumn(
            "rolling_7d_min_observed_hr",
            F.min("daily_min_hr").over(w7)
        )
        .withColumn(
            "cadence_values_28d",
            F.array_sort(
                F.flatten(
                    F.collect_list("daily_z2_cadences").over(w28)
                )
            )
        )
        .withColumn(
            "cadence_count",
            F.size("cadence_values_28d")
        )
        .withColumn(
            "rolling_28d_typical_cadence",
            F.when(
                F.col("cadence_count") > 0,
                (
                    F.element_at(
                        "cadence_values_28d",
                        F.expr("cast(floor((cadence_count + 1) / 2) as int)")
                    ) +
                    F.element_at(
                        "cadence_values_28d",
                        F.expr("cast(floor((cadence_count + 2) / 2) as int)")
                    )
                ) / 2.0
            )
        )
        .select(
            "athlete_id",
            "athlete_name",
            "squad",
            "observation_date",
            "daily_min_hr",
            "rolling_7d_min_observed_hr",
            F.round(
                "rolling_28d_typical_cadence", 1
            ).alias("rolling_28d_typical_cadence"),
            "cadence_count"
        )
        .orderBy("athlete_id", "observation_date")
    )

    # Save Parquet and CSV.
    baseline.write.mode("overwrite").parquet(
        str(GOLD / "mart_athlete_baseline")
    )

    (
        baseline.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(GOLD / "csv_mart_athlete_baseline"))
    )

    print("Athlete baseline mart completed.")
    baseline.show(20, truncate=False)

    return baseline


def main():
    spark = init_spark("StrideAthleteBaseline")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Karachi")

    try:
        build_athlete_baseline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
