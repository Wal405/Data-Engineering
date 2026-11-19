# Databricks notebook source

# PHASE 4 - SILVER FOREIGN-KEY VALIDATION

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
        f"Project folder not found: {notebook_file}"
    )

sys.path.insert(0, str(project_root / "src"))

# 2. Load dataset metadata
from ecommerce.metadata import MetadataRegistry

registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

datasets = registry.all_datasets()

parent_lookup = {
    ds.metadata["silver_table"]: ds
    for ds in datasets
}

results = []

# 3. Validate foreign-key relationships
for ds in datasets:

    foreign_keys = ds.quality_rules.get(
        "foreign_keys", []
    )

    for fk in foreign_keys:

        child_column = fk["column"]
        parent_column = fk["ref_column"]
        parent_ds = parent_lookup[fk["ref_table"]]

        child = spark.table(ds.full_silver_table)
        parent = spark.table(parent_ds.full_silver_table)

        # Only current SCD2 parent versions
        if parent_ds.load_type == "scd2":
            parent = parent.filter(
                F.col("is_current") == True
            )

        # Unique, non-null parent keys
        parent_keys = (
            parent
            .select(
                F.col(parent_column).alias("_parent_key")
            )
            .filter(F.col("_parent_key").isNotNull())
            .distinct()
        )

        # Identify child records without a parent
        orphan_count = (
            child.join(
                parent_keys,
                F.col(child_column) == F.col("_parent_key"),
                "left_anti"
            )
            .count()
        )

        results.append({
            "child_table": ds.name,
            "foreign_key": child_column,
            "parent_table": parent_ds.name,
            "orphan_records": orphan_count,
            "status": (
                "PASS" if orphan_count == 0 else "FAIL"
            )
        })

# 4. Display validation results
if not results:
    raise RuntimeError(
        "No foreign-key relationships configured."
    )

display(spark.createDataFrame(results))

# 5. Calculate validation summary
failures = [
    r for r in results
    if r["status"] == "FAIL"
]

print(f"Relationships checked: {len(results)}")
print(f"Passed: {len(results) - len(failures)}")
print(f"Failed: {len(failures)}")

# 6. Stop downstream jobs if validation fails
if failures:
    raise RuntimeError(
        "Silver foreign-key validation failed: "
        + ", ".join(
            f"{r['child_table']}.{r['foreign_key']}"
            for r in failures
        )
    )

print(
    "SUCCESS: All Silver foreign-key "
    "relationships are valid."
)
