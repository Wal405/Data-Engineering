# Databricks notebook source

import sys
from pathlib import Path

# Locate project root
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)

current = Path("/Workspace" + notebook_path)

project_root = next(
    p for p in current.parents
    if p.name == "enterprise-ecommerce-platform"
)

sys.path.insert(0, str(project_root / "src"))

from ecommerce.metadata import MetadataRegistry
from ecommerce.ingestion import ingest_bronze

# Load metadata
registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

# Run all nine datasets
results = []

for dataset in registry.all_datasets():
    print(f"Processing: {dataset.name}")

    try:
        table = ingest_bronze(spark, dataset)

        results.append({
            "dataset": dataset.name,
            "table": table,
            "status": "SUCCESS",
            "error": ""
        })

        print(f"SUCCESS: {table}")

    except Exception as e:
        results.append({
            "dataset": dataset.name,
            "table": dataset.full_bronze_table,
            "status": "FAILED",
            "error": str(e)[:500]
        })

        print(f"FAILED: {dataset.name} - {e}")

display(spark.createDataFrame(results))

failed = [r for r in results if r["status"] == "FAILED"]

if failed:
    raise RuntimeError(
        f"Bronze ingestion failed for: "
        f"{[r['dataset'] for r in failed]}"
    )

print("All 9 Bronze ingestion jobs completed.")
