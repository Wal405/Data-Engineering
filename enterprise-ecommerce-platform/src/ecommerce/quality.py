
# src/ecommerce/quality.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


QUARANTINE_TABLE = "ecommerce_dev.monitoring.quarantine_records"


# ============================================================
# 1. CREATE QUARANTINE TABLE
# ============================================================

def ensure_quarantine_table(spark, table_name=QUARANTINE_TABLE):
    """Create centralized Delta quarantine table."""

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            quarantine_id STRING,
            run_id STRING,
            dataset_name STRING,
            source_table STRING,
            target_table STRING,
            business_key_value STRING,
            quarantine_reasons ARRAY<STRING>,
            raw_record_json STRING,
            _source_file STRING,
            _ingested_at TIMESTAMP,
            quarantined_at TIMESTAMP
        )
        USING DELTA
    """)


# ============================================================
# 2. METADATA-DRIVEN QUALITY VALIDATION
# ============================================================

def validate_quality(df, dataset):
    """Apply YAML quality rules and generate rejection reasons."""

    rules = dataset.quality_rules
    reasons = []

    # Casting and required-field validation
    reasons.append(
        F.when(
            ~F.coalesce(F.col("_is_valid"), F.lit(False)),
            F.lit("INVALID_CAST_OR_REQUIRED_FIELD")
        )
    )

    # Corrupt and rescued records
    for name in ("_corrupt_record", "_rescued_data"):
        if name in df.columns:
            reasons.append(
                F.when(
                    F.col(name).isNotNull(),
                    F.lit(f"INVALID_SOURCE:{name}")
                )
            )

    # Required fields and blank values
    for name in rules.get("not_null", []):
        reasons.append(
            F.when(
                F.col(name).isNull() |
                (F.trim(F.col(name).cast("string")) == ""),
                F.lit(f"NULL_OR_BLANK:{name}")
            )
        )

    # Non-negative numeric values
    for name in rules.get("non_negative", []):
        reasons.append(
            F.when(
                F.col(name) < 0,
                F.lit(f"NEGATIVE_VALUE:{name}")
            )
        )

    # Allowed categorical values
    for name, values in rules.get("allowed_values", {}).items():
        reasons.append(
            F.when(
                F.col(name).isNotNull() &
                ~F.col(name).isin(list(values)),
                F.lit(f"INVALID_VALUE:{name}")
            )
        )

    # Range and geographic coordinate checks
    ranges = {
        **rules.get("range_checks", {}),
        **rules.get("coordinate_ranges", {})
    }

    for name, limits in ranges.items():
        reasons.append(
            F.when(
                F.col(name).isNotNull() &
                ~F.col(name).between(
                    limits["min"], limits["max"]
                ),
                F.lit(f"OUT_OF_RANGE:{name}")
            )
        )

    # Duplicate business-key checks
    keys = rules.get("unique_keys", [])

    if keys:
        window = Window.partitionBy(*keys)

        df = df.withColumn(
            "_duplicate_count",
            F.count("*").over(window)
        )

        reasons.append(
            F.when(
                F.col("_duplicate_count") > 1,
                F.lit(f"DUPLICATE_KEY:{','.join(keys)}")
            )
        )

    # Generate human-readable rejection reasons
    df = df.withColumn(
        "_dq_reasons",
        F.array_compact(F.array(*reasons))
    )

    # Remove temporary duplicate-count column
    if "_duplicate_count" in df.columns:
        df = df.drop("_duplicate_count")

    return df.withColumn(
        "_quality_pass",
        F.size(F.col("_dq_reasons")) == 0
    )


# ============================================================
# 3. FOREIGN KEY VALIDATION
# ============================================================

def check_foreign_keys(spark, df, dataset):
    """Validate references against existing Silver parent tables."""

    rules = dataset.quality_rules.get("foreign_keys", [])
    result = df

    platform = dataset.config["platform"]
    catalog = platform["catalog"]
    silver_schema = platform["schemas"]["silver"]

    for fk in rules:

        fk_col = fk["column"]
        ref_col = fk["ref_column"]

        ref_table = (
            f"{catalog}.{silver_schema}.{fk['ref_table']}"
        )

        if not spark.catalog.tableExists(ref_table):
            raise RuntimeError(
                f"Parent Silver table missing: {ref_table}"
            )

        parent = (
            spark.table(ref_table)
            .select(F.col(ref_col).alias("_parent_key"))
            .filter(F.col("_parent_key").isNotNull())
            .distinct()
        )

        result = (
            result.join(
                parent,
                result[fk_col] == parent["_parent_key"],
                "left"
            )
            .withColumn(
                "_dq_reasons",
                F.when(
                    F.col(fk_col).isNotNull() &
                    F.col("_parent_key").isNull(),
                    F.array_union(
                        F.col("_dq_reasons"),
                        F.array(
                            F.lit(
                                f"FK_VIOLATION:{fk_col}"
                                f"->{fk['ref_table']}.{ref_col}"
                            )
                        )
                    )
                ).otherwise(F.col("_dq_reasons"))
            )
            .drop("_parent_key")
        )

    return result.withColumn(
        "_quality_pass",
        F.size(F.col("_dq_reasons")) == 0
    )


# ============================================================
# 4. QUARANTINE INVALID RECORDS
# ============================================================

def quarantine_invalid_records(
    spark,
    df_evaluated,
    dataset,
    run_id,
    quarantine_table=QUARANTINE_TABLE
):
    """
    Persist invalid records using Delta MERGE.
    Return valid records and rejected-row count.
    """

    if "_raw_record_json" not in df_evaluated.columns:
        raise ValueError(
            "Missing _raw_record_json. "
            "Preserve Bronze values before casting."
        )

    ensure_quarantine_table(spark, quarantine_table)

    invalid_df = df_evaluated.filter(
        ~F.col("_quality_pass")
    )

    valid_df = (
        df_evaluated
        .filter(F.col("_quality_pass"))
        .drop(
            "_is_valid",
            "_quality_pass",
            "_dq_reasons",
            "_raw_record_json"
        )
    )

    rejected_count = invalid_df.count()

    if rejected_count == 0:
        return valid_df, 0

    # Generate business key representation
    keys = list(dataset.business_keys)

    business_key = F.to_json(
        F.struct(*[
            F.col(k).cast("string").alias(k)
            for k in keys
        ])
    )

    # Preserve source-file metadata
    source_file = (
        F.col("_source_file")
        if "_source_file" in invalid_df.columns
        else F.lit(None).cast("string")
    )

    ingested_at = (
        F.col("_ingested_at")
        if "_ingested_at" in invalid_df.columns
        else F.lit(None).cast("timestamp")
    )

    # Prepare quarantine records
    payload = invalid_df.select(
        F.lit(run_id).alias("run_id"),
        F.lit(dataset.name).alias("dataset_name"),
        F.lit(dataset.full_bronze_table).alias("source_table"),
        F.lit(dataset.full_silver_table).alias("target_table"),
        business_key.alias("business_key_value"),
        F.col("_dq_reasons").alias("quarantine_reasons"),
        F.col("_raw_record_json").alias("raw_record_json"),
        source_file.alias("_source_file"),
        ingested_at.alias("_ingested_at")
    )

    # Deterministic identifier for idempotent retries
    payload = (
        payload.withColumn(
            "quarantine_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        "dataset_name",
                        "_source_file",
                        "raw_record_json",
                        "quarantine_reasons"
                    )
                ),
                256
            )
        )
        .withColumn(
            "quarantined_at",
            F.current_timestamp()
        )
    )

    # Prevent duplicate source keys during MERGE
    payload = payload.dropDuplicates(
        ["quarantine_id"]
    )

    # Insert only previously unseen rejected records
    (
        DeltaTable.forName(spark, quarantine_table)
        .alias("target")
        .merge(
            payload.alias("source"),
            "target.quarantine_id = source.quarantine_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    return valid_df, rejected_count
