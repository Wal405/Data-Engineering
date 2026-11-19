# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import sys
from pathlib import Path

# Get current notebook path
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)

# Locate project root
current = Path("/Workspace" + notebook_path)

project_root = next(
    (p for p in current.parents
     if p.name == "enterprise-ecommerce-platform"),
    None
)

if project_root is None:
    raise ValueError("Project root not found")

# Add src folder to Python import path
src_path = str(project_root / "src")

if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Import project modules
from ecommerce.metadata import MetadataRegistry
from ecommerce.transformations import prepare_silver
from ecommerce.quality import (
    validate_quality,
    quarantine_invalid_records
)

# Load metadata
registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

print("Project root:", project_root)
print("Source path:", src_path)
print("All modules imported successfully!")


# COMMAND ----------


import sys
from pathlib import Path
from pyspark.sql import functions as F

# Resolve project folder
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)

project_root = next(
    p for p in Path("/Workspace" + notebook_path).parents
    if p.name == "enterprise-ecommerce-platform"
)

sys.path.insert(0, str(project_root / "src"))

from ecommerce.metadata import MetadataRegistry
from ecommerce.transformations import prepare_silver

registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

results = []

for ds in registry.all_datasets():

    bronze = spark.table(ds.full_bronze_table)

    silver = prepare_silver(bronze, ds)

    metrics = silver.agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(~F.col("_is_valid"), 1).otherwise(0)
        ).alias("invalid_rows")
    ).first()

    invalid_count = metrics["invalid_rows"]

    results.append({
        "dataset": ds.name,
        "total_rows": metrics["total_rows"],
        "valid_rows": metrics["total_rows"] - invalid_count,
        "invalid_rows": invalid_count,
        "status": (
            "PASS" if invalid_count == 0 else "REVIEW"
        )
    })

display(
    spark.createDataFrame(results).orderBy("dataset")
)


# COMMAND ----------


# PHASE 4 - DATA QUALITY AND QUARANTINE

import uuid
from pyspark.sql import functions as F

from ecommerce.quality import (
    validate_quality,
    quarantine_invalid_records
)

run_id = str(uuid.uuid4())
quality_results = []

for ds in registry.all_datasets():

    # Read Bronze table
    bronze = spark.table(ds.full_bronze_table)

    # Preserve original source values
    source_cols = [
        F.col(f"`{c['name']}`")
        for c in ds.metadata["columns"]
    ]

    bronze = bronze.withColumn(
        "_raw_record_json",
        F.to_json(F.struct(*source_cols))
    )

    # Apply Silver transformations
    silver = prepare_silver(bronze, ds)

    # Apply metadata-driven quality rules
    checked = validate_quality(silver, ds)

    # Save invalid records in quarantine table
    valid_df, rejected = quarantine_invalid_records(
        spark,
        checked,
        ds,
        run_id
    )

    quality_results.append({
        "dataset": ds.name,
        "rejected_rows": rejected,
        "status": "PASS" if rejected == 0 else "REVIEW"
    })

display(
    spark.createDataFrame(quality_results)
    .orderBy("dataset")
)


# COMMAND ----------


# PHASE 4 - REUSABLE SILVER DELTA LOADING
# PHASE 8 - DATASET AUDIT LOGGING

import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

from ecommerce.incremental import load_silver
from ecommerce.monitoring import log_pipeline_audit
from ecommerce.quality import (
    validate_quality,
    quarantine_invalid_records
)

# Reload latest YAML configuration
registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

run_id = str(uuid.uuid4())
results = []

for ds in registry.all_datasets():

    started_at = datetime.now(timezone.utc)
    records_read = None
    rejected = None

    try:
        # 1. Read Bronze records
        bronze = spark.table(ds.full_bronze_table)
        records_read = bronze.count()

        # 2. Preserve original source values
        source_cols = [
            F.col(f"`{c['name']}`")
            for c in ds.metadata["columns"]
        ]

        bronze = bronze.withColumn(
            "_raw_record_json",
            F.to_json(F.struct(*source_cols))
        )

        # 3. Apply Silver transformations
        silver = prepare_silver(bronze, ds)

        # 4. Apply data quality rules
        checked = validate_quality(silver, ds)

        # 5. Quarantine invalid records
        valid_df, rejected = quarantine_invalid_records(
            spark, checked, ds, run_id
        )

        # 6. Load valid records into Silver
        action = load_silver(spark, valid_df, ds)

        # 7. Record successful execution
        log_pipeline_audit(
            spark=spark,
            run_id=run_id,
            dataset_name=ds.name,
            layer="silver",
            source_table=ds.full_bronze_table,
            target_table=ds.full_silver_table,
            started_at=started_at,
            execution_status="SUCCESS",
            records_read=records_read,
            records_written=None,
            records_rejected=rejected
        )

        results.append({
            "dataset": ds.name,
            "action": action,
            "rejected_rows": rejected,
            "status": "SUCCESS",
            "error": ""
        })

    except Exception as e:
        error_message = str(e)[:2000]

        # Record failed execution
        try:
            log_pipeline_audit(
                spark=spark,
                run_id=run_id,
                dataset_name=ds.name,
                layer="silver",
                source_table=ds.full_bronze_table,
                target_table=ds.full_silver_table,
                started_at=started_at,
                execution_status="FAILED",
                records_read=records_read,
                records_written=None,
                records_rejected=rejected,
                error_message=error_message
            )
        except Exception as audit_error:
            print(
                f"Audit logging failed for {ds.name}: "
                f"{audit_error}"
            )

        results.append({
            "dataset": ds.name,
            "action": "FAILED",
            "rejected_rows": (
                rejected if rejected is not None else -1
            ),
            "status": "FAILED",
            "error": error_message[:500]
        })

display(spark.createDataFrame(results))

if any(r["status"] == "FAILED" for r in results):
    raise RuntimeError(
        "Silver loading failed for some datasets"
    )
