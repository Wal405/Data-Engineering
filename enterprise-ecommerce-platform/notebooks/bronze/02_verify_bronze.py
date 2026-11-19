# Databricks notebook source

from pathlib import Path
import sys
from pyspark.sql import functions as F

# Find project root
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

registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

results = []

for ds in registry.all_datasets():

    try:
        # Read source file with the Bronze schema
        source_df = (
            spark.read.format("csv")
            .options(**ds.csv_options)
            .schema(ds.bronze_struct_type)
            .load(ds.source_file_path)
        )

        bronze_df = spark.table(ds.full_bronze_table)

        source_count = source_df.count()
        bronze_count = bronze_df.count()

        # Count rescued and corrupt records if columns exist
        checks = [
            c for c in ("_rescued_data", "_corrupt_record")
            if c in bronze_df.columns
        ]

        counts = (
            bronze_df.agg(*[
                F.count(F.when(F.col(c).isNotNull(), 1)).alias(c)
                for c in checks
            ]).first().asDict()
            if checks else {}
        )

        rescued = counts.get("_rescued_data", 0)
        corrupt = counts.get("_corrupt_record", 0)

        results.append({
            "dataset": ds.name,
            "source_rows": source_count,
            "bronze_rows": bronze_count,
            "rescued_rows": rescued,
            "corrupt_rows": corrupt,
            "status": (
                "PASS"
                if source_count == bronze_count
                and rescued == 0
                and corrupt == 0
                else "REVIEW"
            )
        })

    except Exception as e:
        print(f"ERROR - {ds.name}: {e}")
        results.append({
            "dataset": ds.name,
            "source_rows": -1,
            "bronze_rows": -1,
            "rescued_rows": -1,
            "corrupt_rows": -1,
            "status": "ERROR"
        })

# Display verification summary
display(
    spark.createDataFrame(results).orderBy("dataset")
)

passed = sum(r["status"] == "PASS" for r in results)

print(f"Datasets verified: {passed}/{len(results)}")

if passed != len(results):
    raise ValueError(
        "Some Bronze tables require investigation."
    )

print("SUCCESS: All nine Bronze tables verified.")
