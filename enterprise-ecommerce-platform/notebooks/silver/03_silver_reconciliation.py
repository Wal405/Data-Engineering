# Databricks notebook source

# PHASE 4 - FINAL SILVER RECONCILIATION
# PHASE 6 - LAKEFLOW QUALITY GATE

import sys
from pathlib import Path
from pyspark.sql import functions as F

# 1. Locate project root
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)

notebook_file = Path("/Workspace") / notebook_path.lstrip("/")

project_root = next(
    (
        p for p in notebook_file.parents
        if p.name == "enterprise-ecommerce-platform"
    ),
    None
)

if project_root is None:
    raise RuntimeError(
        f"Project root not found: {notebook_file}"
    )

sys.path.insert(0, str(project_root / "src"))

# 2. Load metadata
from ecommerce.metadata import MetadataRegistry

registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

datasets = registry.all_datasets()
results = []

# 3. Validate all Silver datasets
for ds in datasets:

    bronze = spark.table(ds.full_bronze_table)
    silver = spark.table(ds.full_silver_table)

    # SCD2: Compare only current records
    current = (
        silver.filter(F.col("is_current") == True)
        if ds.load_type == "scd2"
        else silver
    )

    keys = list(ds.business_keys)

    bronze_count = bronze.count()
    silver_count = silver.count()
    current_count = current.count()

    # Reference data: exact duplicates removed in Silver
    if ds.load_type == "overwrite_clean":

        expected_count = (
            bronze.select(
                *[c["name"] for c in ds.metadata["columns"]]
            )
            .distinct()
            .count()
        )

        # Geolocation allows repeated ZIP prefixes
        duplicate_keys = 0

        null_condition = F.col(keys[0]).isNull()
        for key in keys[1:]:
            null_condition = (
                null_condition | F.col(key).isNull()
            )

        null_keys = current.filter(null_condition).count()

    else:
        expected_count = bronze_count

        duplicate_keys = (
            current.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        null_condition = F.col(keys[0]).isNull()
        for key in keys[1:]:
            null_condition = (
                null_condition | F.col(key).isNull()
            )

        null_keys = current.filter(null_condition).count()

    status = (
        "PASS"
        if (
            current_count == expected_count
            and duplicate_keys == 0
            and null_keys == 0
        )
        else "FAIL"
    )

    results.append({
        "dataset": ds.name,
        "load_type": ds.load_type,
        "bronze_rows": bronze_count,
        "silver_rows": silver_count,
        "expected_rows": expected_count,
        "current_rows": current_count,
        "duplicate_keys": duplicate_keys,
        "null_keys": null_keys,
        "status": status
    })

# 4. Display results
if not results:
    raise RuntimeError("No datasets found in metadata registry.")

display(spark.createDataFrame(results))

# 5. Summary
failed = [
    r for r in results
    if r["status"] != "PASS"
]

print(f"Datasets checked: {len(results)}")
print(f"Passed: {len(results) - len(failed)}")
print(f"Failed: {len(failed)}")

# 6. Stop Lakeflow Jobs when validation fails
if len(results) != 9:
    raise RuntimeError(
        f"Expected 9 datasets, checked {len(results)}"
    )

if failed:
    raise RuntimeError(
        "Silver reconciliation failed: "
        + ", ".join(r["dataset"] for r in failed)
    )

print("SUCCESS: All 9 Silver datasets reconciled.")
